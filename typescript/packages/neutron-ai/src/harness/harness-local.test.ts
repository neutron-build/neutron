import assert from "node:assert/strict";
import { test } from "node:test";
import { z } from "zod";

import type { AdapterCallOptions, AdapterStreamPart, ModelAdapter } from "../adapter.js";
import { tool } from "../tool.js";
import type { AgentEvent } from "./index.js";
import { localAgent } from "./index.js";

function scriptedStreamModel(script: AdapterStreamPart[][]): { model: ModelAdapter; calls: AdapterCallOptions[] } {
  const calls: AdapterCallOptions[] = [];
  let index = 0;
  return {
    calls,
    model: {
      provider: "scripted",
      modelId: "scripted-1",
      async doGenerate() {
        throw new Error("not scripted");
      },
      async *doStream(options) {
        calls.push(structuredClone(options));
        if (index >= script.length) throw new Error("script exhausted");
        for (const part of script[index++]!) yield part;
      },
    },
  };
}

const usage = (i: number, o: number) => ({ inputTokens: i, outputTokens: o, totalTokens: i + o });

const echo = tool({
  name: "echo",
  inputSchema: z.object({ value: z.string() }),
  execute: async ({ value }) => ({ echoed: value }),
});

const danger = tool({
  name: "danger",
  inputSchema: z.object({}),
  execute: async () => "done",
  needsApproval: true,
});

test("localAgent runs the tool loop and emits harness events", async () => {
  const { model } = scriptedStreamModel([
    [
      { type: "text-delta", text: "Checking. " },
      { type: "tool-call", toolCallId: "c1", toolName: "echo", input: { value: "hi" } },
      { type: "finish", finishReason: "tool-calls", usage: usage(10, 5) },
    ],
    [
      { type: "text-delta", text: "All good." },
      { type: "finish", finishReason: "stop", usage: usage(20, 8) },
    ],
  ]);
  const agent = localAgent({ model, tools: [echo], maxSteps: 3 });
  const run = agent.run({ prompt: "check things" });

  const events: AgentEvent[] = [];
  for await (const event of run.events) events.push(event);

  assert.equal(events[0]?.type, "session");
  assert.deepEqual(events.slice(1), [
    { type: "text-delta", text: "Checking. " },
    { type: "tool-start", toolCallId: "c1", toolName: "echo", input: { value: "hi" } },
    { type: "tool-end", toolCallId: "c1", toolName: "echo", output: { echoed: "hi" } },
    { type: "text-delta", text: "All good." },
    { type: "finish", status: "completed" },
  ]);

  const result = await run.result;
  assert.equal(result.status, "completed");
  assert.equal(result.output, "Checking. All good.");
  assert.deepEqual(result.usage, usage(30, 13));
  assert.ok(result.sessionId);
});

test("sessions persist across runs and resume with approvals", async () => {
  const { model, calls } = scriptedStreamModel([
    [
      { type: "tool-call", toolCallId: "c1", toolName: "danger", input: {} },
      { type: "finish", finishReason: "tool-calls", usage: usage(5, 2) },
    ],
    [
      { type: "text-delta", text: "done" },
      { type: "finish", finishReason: "stop", usage: usage(7, 3) },
    ],
  ]);
  const agent = localAgent({ model, tools: [danger], maxSteps: 3 });

  const first = agent.run({ prompt: "do it" });
  const firstResult = await first.result;
  assert.equal(firstResult.status, "suspended");
  assert.equal(firstResult.approvalRequests?.length, 1);

  const second = agent.run({
    sessionId: firstResult.sessionId!,
    toolApprovals: [{ toolCallId: "c1", approved: true }],
  });
  const secondResult = await second.result;
  assert.equal(secondResult.status, "completed");
  assert.equal(secondResult.output, "done");
  assert.equal(secondResult.sessionId, firstResult.sessionId);

  // the resumed call saw the prior conversation plus the executed tool result
  const resumedMessages = calls[1]!.messages;
  assert.equal(resumedMessages[0]?.role, "user");
  assert.equal(resumedMessages[1]?.role, "assistant");
  assert.equal(resumedMessages[2]?.role, "tool");
});

test("onApprovalRequest handles approvals inline", async () => {
  const { model } = scriptedStreamModel([
    [
      { type: "tool-call", toolCallId: "c1", toolName: "danger", input: {} },
      { type: "finish", finishReason: "tool-calls", usage: usage(5, 2) },
    ],
    [
      { type: "text-delta", text: "done" },
      { type: "finish", finishReason: "stop", usage: usage(7, 3) },
    ],
  ]);
  const agent = localAgent({ model, tools: [danger], maxSteps: 3 });
  const run = agent.run({ prompt: "do it", onApprovalRequest: () => true });
  const result = await run.result;
  assert.equal(result.status, "completed");
  assert.equal(result.output, "done");
});

test("stop() cancels the run", async () => {
  const model: ModelAdapter = {
    provider: "hanging",
    modelId: "hanging-1",
    async doGenerate() {
      throw new Error("not used");
    },
    async *doStream(options) {
      yield { type: "text-delta", text: "partial" };
      await new Promise((_resolve, reject) => {
        options.abortSignal?.addEventListener("abort", () => reject(new Error("aborted")));
      });
    },
  };
  const agent = localAgent({ model });
  const run = agent.run({ prompt: "hang" });
  const resultPromise = run.result;
  setTimeout(() => run.stop(), 10);
  const result = await resultPromise;
  assert.equal(result.status, "cancelled");
  assert.equal(result.output, "partial");
});

test("errors surface as a finish event and result.error, not throws", async () => {
  const model: ModelAdapter = {
    provider: "broken",
    modelId: "broken-1",
    async doGenerate() {
      throw new Error("not used");
    },
    // eslint-disable-next-line require-yield
    async *doStream() {
      throw new Error("model exploded");
    },
  };
  const agent = localAgent({ model });
  const run = agent.run({ prompt: "go" });
  const events: AgentEvent[] = [];
  for await (const event of run.events) events.push(event);
  assert.deepEqual(events.at(-1), { type: "finish", status: "error" });
  const result = await run.result;
  assert.equal(result.status, "error");
  assert.match(result.error?.detail ?? "", /model exploded/);
});

test("running without prompt or session throws", () => {
  const { model } = scriptedStreamModel([]);
  const agent = localAgent({ model });
  assert.throws(() => agent.run({}));
});
