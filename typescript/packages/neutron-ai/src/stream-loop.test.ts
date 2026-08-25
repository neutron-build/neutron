import assert from "node:assert/strict";
import { test } from "node:test";
import { z } from "zod";

import type { AdapterCallOptions, AdapterStreamPart, ModelAdapter } from "./adapter.js";
import { AIError } from "./errors.js";
import { streamText } from "./stream-text.js";
import { tool } from "./tool.js";
import type { StreamPart } from "./types.js";

function scriptedStreamModel(script: AdapterStreamPart[][]): {
  model: ModelAdapter;
  calls: AdapterCallOptions[];
} {
  const calls: AdapterCallOptions[] = [];
  let index = 0;
  return {
    calls,
    model: {
      provider: "scripted",
      modelId: "scripted-1",
      async doGenerate() {
        throw new Error("doGenerate not scripted");
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
  execute: async () => "did it",
  needsApproval: true,
});

test("streams a two-step tool loop with step-finish and total usage", async () => {
  const { model } = scriptedStreamModel([
    [
      { type: "text-delta", text: "Let me check. " },
      { type: "tool-call", toolCallId: "c1", toolName: "echo", input: { value: "hi" } },
      { type: "finish", finishReason: "tool-calls", usage: usage(10, 5) },
    ],
    [
      { type: "text-delta", text: "Done." },
      { type: "finish", finishReason: "stop", usage: usage(20, 8) },
    ],
  ]);
  const result = streamText({ model, prompt: "go", tools: [echo], maxSteps: 3 });
  const parts: StreamPart[] = [];
  for await (const part of result.fullStream) parts.push(part);

  assert.deepEqual(parts, [
    { type: "text-delta", text: "Let me check. " },
    { type: "tool-call", toolCallId: "c1", toolName: "echo", input: { value: "hi" } },
    { type: "tool-result", toolCallId: "c1", toolName: "echo", output: { echoed: "hi" } },
    { type: "step-finish", finishReason: "tool-calls", usage: usage(10, 5) },
    { type: "text-delta", text: "Done." },
    { type: "finish", finishReason: "stop", usage: usage(30, 13) },
  ]);
  assert.equal(await result.text, "Let me check. Done.");
  assert.equal(await result.finishReason, "stop");
  assert.equal((await result.steps).length, 2);
  assert.equal((await result.messages).length, 4);
});

test("streaming suspends on approval with an approval-request part", async () => {
  const { model } = scriptedStreamModel([
    [
      { type: "tool-call", toolCallId: "c1", toolName: "danger", input: {} },
      { type: "finish", finishReason: "tool-calls", usage: usage(5, 2) },
    ],
  ]);
  const result = streamText({ model, prompt: "go", tools: [danger], maxSteps: 3 });
  const parts: StreamPart[] = [];
  for await (const part of result.fullStream) parts.push(part);

  assert.deepEqual(parts, [
    { type: "tool-call", toolCallId: "c1", toolName: "danger", input: {} },
    { type: "approval-request", request: { toolCallId: "c1", toolName: "danger", input: {} } },
    { type: "finish", finishReason: "tool-approval", usage: usage(5, 2) },
  ]);
  assert.deepEqual(await result.approvalRequests, [{ toolCallId: "c1", toolName: "danger", input: {} }]);
  assert.equal(await result.finishReason, "tool-approval");
});

test("streams retry only before any output was produced", async () => {
  let attempts = 0;
  const model: ModelAdapter = {
    provider: "flaky",
    modelId: "flaky-1",
    async doGenerate() {
      throw new Error("not used");
    },
    async *doStream() {
      attempts += 1;
      if (attempts === 1) {
        throw new AIError({ type: "https://neutron.dev/errors/internal", title: "Internal Server Error", status: 500, detail: "overloaded" });
      }
      yield { type: "text-delta", text: "ok" };
      yield { type: "finish", finishReason: "stop", usage: usage(1, 1) };
    },
  };
  const result = streamText({ model, prompt: "go", retryDelayMs: 1 });
  assert.equal(await result.text, "ok");
  assert.equal(attempts, 2);
});

test("streams do not retry after output was produced", async () => {
  let attempts = 0;
  const model: ModelAdapter = {
    provider: "flaky",
    modelId: "flaky-1",
    async doGenerate() {
      throw new Error("not used");
    },
    async *doStream() {
      attempts += 1;
      yield { type: "text-delta", text: "partial" };
      throw new AIError({ type: "https://neutron.dev/errors/internal", title: "Internal Server Error", status: 500, detail: "mid-stream" });
    },
  };
  const result = streamText({ model, prompt: "go", retryDelayMs: 1 });
  await assert.rejects(result.text, /mid-stream/);
  assert.equal(attempts, 1);
});

test("resuming a suspended run streams the held tool result first", async () => {
  const first = scriptedStreamModel([
    [
      { type: "tool-call", toolCallId: "c1", toolName: "danger", input: {} },
      { type: "finish", finishReason: "tool-calls", usage: usage(5, 2) },
    ],
  ]);
  const suspended = streamText({ model: first.model, prompt: "go", tools: [danger], maxSteps: 3 });
  const priorMessages = await suspended.messages;

  const second = scriptedStreamModel([
    [
      { type: "text-delta", text: "done" },
      { type: "finish", finishReason: "stop", usage: usage(7, 3) },
    ],
  ]);
  const resumed = streamText({
    model: second.model,
    messages: priorMessages,
    tools: [danger],
    maxSteps: 3,
    toolApprovals: [{ toolCallId: "c1", approved: true }],
  });
  const parts: StreamPart[] = [];
  for await (const part of resumed.fullStream) parts.push(part);

  assert.deepEqual(parts, [
    { type: "tool-result", toolCallId: "c1", toolName: "danger", output: "did it" },
    { type: "text-delta", text: "done" },
    { type: "finish", finishReason: "stop", usage: usage(7, 3) },
  ]);
  assert.equal(await resumed.text, "done");
});

test("an abandoned stream settles result promises instead of hanging", async () => {
  const { model } = scriptedStreamModel([
    [
      { type: "text-delta", text: "partial output" },
      { type: "finish", finishReason: "stop", usage: usage(2, 2) },
    ],
  ]);
  const result = streamText({ model, prompt: "go" });

  for await (const _part of result.textStream) {
    break; // abandon after the first delta
  }

  // Pre-fix witness: the promise never settled at all — race a deadline so
  // the failure mode is a fast assertion, not a hung test process.
  let timer: ReturnType<typeof setTimeout> | undefined;
  const deadline = new Promise<"hang">((resolve) => {
    timer = setTimeout(() => resolve("hang"), 500);
  });
  const outcome = await Promise.race([
    result.text.then(
      () => "settled" as const,
      () => "settled" as const,
    ),
    deadline,
  ]);
  clearTimeout(timer);
  assert.notEqual(
    outcome,
    "hang",
    "result.text must settle after the stream is abandoned; it used to hang forever",
  );
  await assert.rejects(result.text, /abandoned/);
});
