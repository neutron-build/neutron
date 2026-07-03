import assert from "node:assert/strict";
import { test } from "node:test";
import { z } from "zod";

import type { AdapterCallOptions, AdapterGenerateResult, ModelAdapter } from "./adapter.js";
import { AIError } from "./errors.js";
import { generateObject } from "./generate-object.js";

function fakeModel(result: AdapterGenerateResult): { model: ModelAdapter; calls: AdapterCallOptions[] } {
  const calls: AdapterCallOptions[] = [];
  return {
    calls,
    model: {
      provider: "fake",
      modelId: "fake-1",
      async doGenerate(options) {
        calls.push(structuredClone(options));
        return result;
      },
      async *doStream() {
        throw new Error("not used");
      },
    },
  };
}

const usage = { inputTokens: 9, outputTokens: 4, totalTokens: 13 };

test("returns the validated, typed object from a forced tool call", async () => {
  const { model, calls } = fakeModel({
    content: [{ type: "tool-call", toolCallId: "c1", toolName: "json", input: { title: "Neutron", stars: 5 } }],
    finishReason: "tool-calls",
    usage,
    raw: null,
  });
  const result = await generateObject({
    model,
    prompt: "review",
    schema: z.object({ title: z.string(), stars: z.number().int() }),
  });

  assert.deepEqual(result.object, { title: "Neutron", stars: 5 });
  assert.equal(result.finishReason, "stop");
  assert.deepEqual(result.usage, usage);

  const sent = calls[0]!;
  assert.deepEqual(sent.toolChoice, { toolName: "json" });
  assert.equal(sent.tools?.[0]?.name, "json");
  assert.deepEqual(sent.tools?.[0]?.inputSchema, {
    type: "object",
    properties: { title: { type: "string" }, stars: { type: "integer" } },
    additionalProperties: false,
    required: ["title", "stars"],
  });
});

test("schema violations throw a validation problem", async () => {
  const { model } = fakeModel({
    content: [{ type: "tool-call", toolCallId: "c1", toolName: "json", input: { title: 42 } }],
    finishReason: "tool-calls",
    usage,
    raw: null,
  });
  await assert.rejects(
    generateObject({ model, prompt: "review", schema: z.object({ title: z.string() }) }),
    (error: unknown) =>
      error instanceof AIError &&
      error.problem.status === 422 &&
      /title/.test(error.problem.detail),
  );
});

test("a model that produced no structured output throws", async () => {
  const { model } = fakeModel({
    content: [{ type: "text", text: "I cannot." }],
    finishReason: "stop",
    usage,
    raw: null,
  });
  await assert.rejects(
    generateObject({ model, prompt: "review", schema: z.object({ title: z.string() }) }),
    (error: unknown) => error instanceof AIError && error.problem.status === 500,
  );
});

test("schemaName and schemaDescription surface on the wire tool", async () => {
  const { model, calls } = fakeModel({
    content: [{ type: "tool-call", toolCallId: "c1", toolName: "review", input: { title: "x" } }],
    finishReason: "tool-calls",
    usage,
    raw: null,
  });
  await generateObject({
    model,
    prompt: "review",
    schema: z.object({ title: z.string() }),
    schemaName: "review",
    schemaDescription: "A product review.",
  });
  assert.equal(calls[0]?.tools?.[0]?.name, "review");
  assert.equal(calls[0]?.tools?.[0]?.description, "A product review.");
  assert.deepEqual(calls[0]?.toolChoice, { toolName: "review" });
});
