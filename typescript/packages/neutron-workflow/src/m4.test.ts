import assert from "node:assert/strict";
import { test } from "node:test";
import { setTimeout as delay } from "node:timers/promises";

import type { AdapterCallOptions, AdapterGenerateResult, ModelAdapter, Tool } from "@neutron-build/ai";
import { jsonSchema } from "@neutron-build/ai";

import { agentStep, approvalEventName } from "./ai/index.js";
import { MemoryEventStore } from "./store.js";
import { completeSleep, deliverEvent, executeRun } from "./run.js";
import { RunIndex, type DocumentLike } from "./scheduler.js";
import { workflow } from "./workflow.js";

test("in-process retries re-attempt, record every attempt, and never re-run on replay", async () => {
  let executions = 0;
  const wf = workflow("flaky", async (ctx) => {
    const value = await ctx.step(
      "wobbly",
      () => {
        executions += 1;
        if (executions < 3) throw new Error(`boom ${executions}`);
        return "steady";
      },
      { retries: 2 },
    );
    await ctx.sleep("1h");
    return value;
  });

  const store = new MemoryEventStore();
  const first = await executeRun({ workflow: wf, runId: "run-1", store, input: null });
  assert.equal(first.status, "sleeping");
  assert.equal(executions, 3);

  await completeSleep(store, "run-1");
  const second = await executeRun({ workflow: wf, runId: "run-1", store });
  assert.equal(second.status, "completed");
  assert.equal(second.output, "steady");
  assert.equal(executions, 3); // replay consumed both failed attempts

  const failures = (await store.load("run-1")).filter((e) => e.type === "step-failed");
  assert.deepEqual(failures.map((e) => (e.data as { attempt: number }).attempt), [1, 2]);
});

test("exhausted retries fail the run with the last error", async () => {
  let executions = 0;
  const wf = workflow("doomed", async (ctx) =>
    ctx.step(
      "hopeless",
      () => {
        executions += 1;
        throw new Error("always down");
      },
      { retries: 1 },
    ),
  );
  const store = new MemoryEventStore();
  const outcome = await executeRun({ workflow: wf, runId: "run-1", store, input: null });
  assert.equal(outcome.status, "failed");
  assert.match(outcome.error?.detail ?? "", /always down/);
  assert.equal(executions, 2);
});

test("a retryDelay parks the run durably and the next pass resumes at the next attempt", async () => {
  let executions = 0;
  const wf = workflow("patient", async (ctx) =>
    ctx.step(
      "eventually",
      () => {
        executions += 1;
        if (executions === 1) throw new Error("first try down");
        return "second try up";
      },
      { retries: 1, retryDelay: "5m" },
    ),
  );

  const store = new MemoryEventStore();
  const first = await executeRun({ workflow: wf, runId: "run-1", store, input: null });
  assert.equal(first.status, "retrying");
  assert.ok(first.wakeAt);
  assert.equal(executions, 1);

  const second = await executeRun({ workflow: wf, runId: "run-1", store });
  assert.equal(second.status, "completed");
  assert.equal(second.output, "second try up");
  assert.equal(executions, 2);
});

test("the run index surfaces due retrying runs without a sleep to complete", async () => {
  const docs: Array<Record<string, unknown>> = [];
  const fake: DocumentLike = {
    async insert(_c, doc) {
      docs.push({ ...doc });
      return docs.length;
    },
    async find(_c, filter) {
      return docs.filter((d) => Object.entries(filter).every(([k, v]) => d[k] === v)).map((d) => ({ ...d }));
    },
    async update() {
      return 0;
    },
  };
  const index = new RunIndex(fake);
  await index.record("run-r", "wf", { status: "retrying", wakeAt: new Date(Date.now() - 1000).toISOString() });
  const due = await index.due(new Date());
  assert.deepEqual(due, [{ runId: "run-r", sleeping: false }]);
});

test("a step timeout counts as a failed attempt", async () => {
  const wf = workflow("slow", async (ctx) =>
    ctx.step("stuck", () => new Promise<never>(() => {}), { timeout: "50ms" }),
  );
  const store = new MemoryEventStore();
  const outcome = await executeRun({ workflow: wf, runId: "run-1", store, input: null });
  assert.equal(outcome.status, "failed");
  assert.match(outcome.error?.detail ?? "", /Timed out after 50ms/);
});

test("a run budget fails the run on its next pass, across suspensions", async () => {
  const wf = workflow(
    "budgeted",
    async (ctx) => {
      await ctx.waitForEvent("never-comes");
      return "unreachable";
    },
    { timeout: "100ms" },
  );
  const store = new MemoryEventStore();
  const first = await executeRun({ workflow: wf, runId: "run-1", store, input: null });
  assert.equal(first.status, "waiting");

  await delay(150);
  const second = await executeRun({ workflow: wf, runId: "run-1", store });
  assert.equal(second.status, "failed");
  assert.match(second.error?.detail ?? "", /budget/);

  // terminal thereafter
  const third = await executeRun({ workflow: wf, runId: "run-1", store });
  assert.equal(third.status, "failed");
});

// ---------------------------------------------------------------------------
// The AI SDK approval bridge
// ---------------------------------------------------------------------------

function scriptedModel(script: AdapterGenerateResult[]): { model: ModelAdapter; calls: AdapterCallOptions[] } {
  const calls: AdapterCallOptions[] = [];
  let index = 0;
  return {
    calls,
    model: {
      provider: "scripted",
      modelId: "scripted-1",
      async doGenerate(options) {
        calls.push(structuredClone(options));
        if (index >= script.length) throw new Error("script exhausted");
        return script[index++]!;
      },
      async *doStream() {
        throw new Error("not used");
      },
    },
  };
}

const usage = (i: number, o: number) => ({ inputTokens: i, outputTokens: o, totalTokens: i + o });

test("agentStep suspends on tool approval, resumes on the delivered decision, and never re-calls the model on replay", async () => {
  let deployments = 0;
  const deploy: Tool = {
    name: "deploy",
    inputSchema: jsonSchema({ type: "object" }),
    execute: async () => {
      deployments += 1;
      return "deployed to prod";
    },
    needsApproval: true,
  };

  const { model, calls } = scriptedModel([
    {
      content: [{ type: "tool-call", toolCallId: "c1", toolName: "deploy", input: {} }],
      finishReason: "tool-calls",
      usage: usage(10, 5),
      raw: null,
    },
    {
      content: [{ type: "text", text: "Deployed. All green." }],
      finishReason: "stop",
      usage: usage(20, 8),
      raw: null,
    },
  ]);

  const wf = workflow("release", async (ctx) => {
    const result = await agentStep(ctx, "agent", {
      model,
      prompt: "ship the release",
      tools: [deploy],
      maxSteps: 4,
    });
    return { text: result.text, rounds: result.rounds };
  });

  const store = new MemoryEventStore();
  const first = await executeRun({ workflow: wf, runId: "run-1", store, input: null });
  assert.equal(first.status, "waiting");
  assert.equal(first.eventName, approvalEventName("agent", 0));
  assert.equal(deployments, 0); // nothing executed without approval
  assert.equal(calls.length, 1);

  // the recorded round-0 step carries the pending requests for a UI to render
  const round0 = (await store.load("run-1")).find((e) => e.type === "step-completed");
  const recorded = (round0?.data as { result: { approvalRequests: Array<{ toolName: string }> } }).result;
  assert.equal(recorded.approvalRequests[0]?.toolName, "deploy");

  // days later: the human approves
  await deliverEvent(store, "run-1", approvalEventName("agent", 0), [{ toolCallId: "c1", approved: true }]);
  const second = await executeRun({ workflow: wf, runId: "run-1", store });
  assert.equal(second.status, "completed");
  assert.deepEqual(second.output, { text: "Deployed. All green.", rounds: 2 });
  assert.equal(deployments, 1);
  assert.equal(calls.length, 2); // round 0 replayed from the log, not re-called

  // and the resumed model call saw the executed tool result in the conversation
  const resumed = calls[1]!;
  assert.equal(resumed.messages.some((m) => m.role === "tool"), true);
});

test("agentStep feeds denials back and completes without executing the tool", async () => {
  let deployments = 0;
  const deploy: Tool = {
    name: "deploy",
    inputSchema: jsonSchema({ type: "object" }),
    execute: async () => {
      deployments += 1;
      return "deployed";
    },
    needsApproval: true,
  };
  const { model } = scriptedModel([
    {
      content: [{ type: "tool-call", toolCallId: "c1", toolName: "deploy", input: {} }],
      finishReason: "tool-calls",
      usage: usage(10, 5),
      raw: null,
    },
    {
      content: [{ type: "text", text: "Understood, holding off." }],
      finishReason: "stop",
      usage: usage(20, 8),
      raw: null,
    },
  ]);

  const wf = workflow("release", async (ctx) => {
    const result = await agentStep(ctx, "agent", { model, prompt: "ship it", tools: [deploy], maxSteps: 4 });
    return result.text;
  });

  const store = new MemoryEventStore();
  await executeRun({ workflow: wf, runId: "run-1", store, input: null });
  await deliverEvent(store, "run-1", approvalEventName("agent", 0), [
    { toolCallId: "c1", approved: false, reason: "not during the demo" },
  ]);
  const outcome = await executeRun({ workflow: wf, runId: "run-1", store });
  assert.equal(outcome.status, "completed");
  assert.equal(outcome.output, "Understood, holding off.");
  assert.equal(deployments, 0);
});
