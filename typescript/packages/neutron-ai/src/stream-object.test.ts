import assert from "node:assert/strict";
import { test } from "node:test";
import { z } from "zod";

import type { AdapterCallOptions, AdapterStreamPart, ModelAdapter } from "./adapter.js";
import { AIError } from "./errors.js";
import { streamObject } from "./stream-object.js";

function scriptedStreamModel(parts: AdapterStreamPart[]): { model: ModelAdapter; calls: AdapterCallOptions[] } {
  const calls: AdapterCallOptions[] = [];
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
        for (const part of parts) yield part;
      },
    },
  };
}

const usage = { inputTokens: 12, outputTokens: 6, totalTokens: 18 };

test("streams growing partial objects and resolves the validated final object", async () => {
  const { model, calls } = scriptedStreamModel([
    { type: "tool-input-start", toolCallId: "c1", toolName: "json" },
    { type: "tool-input-delta", toolCallId: "c1", delta: '{"title":"Neu' },
    { type: "tool-input-delta", toolCallId: "c1", delta: 'tron","stars":' },
    { type: "tool-input-delta", toolCallId: "c1", delta: "5}" },
    { type: "tool-call", toolCallId: "c1", toolName: "json", input: { title: "Neutron", stars: 5 } },
    { type: "finish", finishReason: "tool-calls", usage },
  ]);

  const result = streamObject({
    model,
    prompt: "review",
    schema: z.object({ title: z.string(), stars: z.number().int() }),
  });

  const partials: unknown[] = [];
  for await (const partial of result.partialObjectStream) partials.push(partial);

  assert.deepEqual(partials, [
    { title: "Neu" },
    { title: "Neutron" },
    { title: "Neutron", stars: 5 },
  ]);
  assert.deepEqual(await result.object, { title: "Neutron", stars: 5 });
  assert.deepEqual(await result.usage, usage);

  assert.deepEqual(calls[0]?.toolChoice, { toolName: "json" });
  assert.equal(calls[0]?.tools?.[0]?.name, "json");
});

test("awaiting object without consuming the stream drains in the background", async () => {
  const { model } = scriptedStreamModel([
    { type: "tool-input-delta", toolCallId: "c1", delta: '{"title":"x"}' },
    { type: "tool-call", toolCallId: "c1", toolName: "json", input: { title: "x" } },
    { type: "finish", finishReason: "tool-calls", usage },
  ]);
  const result = streamObject({ model, prompt: "go", schema: z.object({ title: z.string() }) });
  assert.deepEqual(await result.object, { title: "x" });
});

test("a final object that fails validation rejects", async () => {
  const { model } = scriptedStreamModel([
    { type: "tool-call", toolCallId: "c1", toolName: "json", input: { title: 42 } },
    { type: "finish", finishReason: "tool-calls", usage },
  ]);
  const result = streamObject({ model, prompt: "go", schema: z.object({ title: z.string() }) });
  await assert.rejects(
    result.object,
    (error: unknown) => error instanceof AIError && error.problem.status === 422,
  );
});

test("a stream with no structured output rejects", async () => {
  const { model } = scriptedStreamModel([
    { type: "text-delta", text: "cannot" },
    { type: "finish", finishReason: "stop", usage },
  ]);
  const result = streamObject({ model, prompt: "go", schema: z.object({ title: z.string() }) });
  await assert.rejects(
    result.object,
    (error: unknown) => error instanceof AIError && error.problem.status === 500,
  );
});
