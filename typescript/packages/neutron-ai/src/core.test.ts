import assert from "node:assert/strict";
import { test } from "node:test";

import type { AdapterCallOptions, AdapterGenerateResult, AdapterStreamPart, ModelAdapter } from "./adapter.js";
import { AIError } from "./errors.js";
import { generateText } from "./generate-text.js";
import { streamText } from "./stream-text.js";
import type { StreamPart } from "./types.js";

const ZERO_USAGE = { inputTokens: 0, outputTokens: 0, totalTokens: 0 };

function fakeModel(config: {
  parts?: AdapterStreamPart[];
  result?: AdapterGenerateResult;
  failAfter?: number;
} = {}): { model: ModelAdapter; calls: AdapterCallOptions[] } {
  const calls: AdapterCallOptions[] = [];
  const model: ModelAdapter = {
    provider: "fake",
    modelId: "fake-1",
    async doGenerate(options) {
      calls.push(structuredClone(options));
      return config.result ?? { content: [], finishReason: "stop", usage: ZERO_USAGE, raw: null };
    },
    async *doStream(options) {
      calls.push(structuredClone(options));
      let index = 0;
      for (const part of config.parts ?? []) {
        if (config.failAfter !== undefined && index === config.failAfter) throw new Error("boom");
        index += 1;
        yield part;
      }
      if (config.failAfter !== undefined && index === config.failAfter) throw new Error("boom");
    },
  };
  return { model, calls };
}

test("generateText turns prompt and system into messages", async () => {
  const { model, calls } = fakeModel();
  await generateText({ model, system: "be brief", prompt: "hi" });
  assert.deepEqual(calls[0]?.messages, [
    { role: "system", content: "be brief" },
    { role: "user", content: "hi" },
  ]);
});

test("generateText rejects prompt and messages together, or neither", async () => {
  const { model } = fakeModel();
  await assert.rejects(
    generateText({ model, prompt: "hi", messages: [{ role: "user", content: "hi" }] }),
    (error: unknown) => error instanceof AIError && error.problem.status === 400,
  );
  await assert.rejects(
    generateText({ model }),
    (error: unknown) => error instanceof AIError && error.problem.status === 400,
  );
});

test("generateText concatenates text parts and extracts tool calls", async () => {
  const { model } = fakeModel({
    result: {
      content: [
        { type: "text", text: "Hello " },
        { type: "tool-call", toolCallId: "t1", toolName: "search", input: { q: "x" } },
        { type: "text", text: "world" },
      ],
      finishReason: "tool-calls",
      usage: { inputTokens: 10, outputTokens: 5, totalTokens: 15 },
      raw: { ok: true },
    },
  });
  const result = await generateText({ model, prompt: "hi" });
  assert.equal(result.text, "Hello world");
  assert.equal(result.toolCalls.length, 1);
  assert.equal(result.toolCalls[0]?.toolName, "search");
  assert.equal(result.finishReason, "tool-calls");
  assert.deepEqual(result.usage, { inputTokens: 10, outputTokens: 5, totalTokens: 15 });
});

const STREAM_PARTS: AdapterStreamPart[] = [
  { type: "text-delta", text: "Hel" },
  { type: "text-delta", text: "lo" },
  { type: "tool-call", toolCallId: "t1", toolName: "search", input: { q: "x" } },
  { type: "finish", finishReason: "tool-calls", usage: { inputTokens: 3, outputTokens: 7, totalTokens: 10 } },
];

test("streamText fullStream yields every part and settles the promises", async () => {
  const { model } = fakeModel({ parts: STREAM_PARTS });
  const result = streamText({ model, prompt: "hi" });
  const seen: StreamPart[] = [];
  for await (const part of result.fullStream) seen.push(part);
  assert.deepEqual(seen, STREAM_PARTS);
  assert.equal(await result.text, "Hello");
  assert.equal((await result.toolCalls).length, 1);
  assert.equal(await result.finishReason, "tool-calls");
  assert.deepEqual(await result.usage, { inputTokens: 3, outputTokens: 7, totalTokens: 10 });
});

test("streamText textStream yields only text deltas", async () => {
  const { model } = fakeModel({ parts: STREAM_PARTS });
  const result = streamText({ model, prompt: "hi" });
  const chunks: string[] = [];
  for await (const chunk of result.textStream) chunks.push(chunk);
  assert.deepEqual(chunks, ["Hel", "lo"]);
});

test("awaiting text without consuming a stream drains in the background", async () => {
  const { model } = fakeModel({ parts: STREAM_PARTS });
  const result = streamText({ model, prompt: "hi" });
  assert.equal(await result.text, "Hello");
  assert.equal(await result.finishReason, "tool-calls");
});

test("a stream missing its finish part settles with defaults", async () => {
  const { model } = fakeModel({ parts: [{ type: "text-delta", text: "hi" }] });
  const result = streamText({ model, prompt: "hi" });
  assert.equal(await result.text, "hi");
  assert.equal(await result.finishReason, "other");
  assert.deepEqual(await result.usage, ZERO_USAGE);
});

test("consuming the stream twice throws", async () => {
  const { model } = fakeModel({ parts: STREAM_PARTS });
  const result = streamText({ model, prompt: "hi" });
  for await (const _chunk of result.textStream) {
    // drain
  }
  assert.throws(
    () => result.fullStream,
    (error: unknown) => error instanceof AIError && error.problem.status === 400,
  );
});

test("a mid-stream error rejects the iterator and the promises", async () => {
  const { model } = fakeModel({ parts: STREAM_PARTS, failAfter: 2 });
  const result = streamText({ model, prompt: "hi" });
  await assert.rejects(async () => {
    for await (const _part of result.fullStream) {
      // consume until the throw
    }
  }, /boom/);
  await assert.rejects(result.text, /boom/);
  await assert.rejects(result.finishReason, /boom/);
});
