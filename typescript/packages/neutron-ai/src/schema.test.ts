import assert from "node:assert/strict";
import { test } from "node:test";
import { z as z3 } from "zod";
import { z as z4 } from "zod/v4";

import { AIError } from "./errors.js";
import { jsonSchema, resolveSchema } from "./schema.js";

test("converts a zod v3 object schema to JSON Schema", () => {
  const schema = resolveSchema(
    z3.object({
      query: z3.string().describe("What to search for"),
      limit: z3.number().int().optional(),
      exact: z3.boolean().default(false),
      kind: z3.enum(["code", "docs"]),
      tags: z3.array(z3.string()),
      filter: z3.object({ path: z3.string().nullable() }),
      mode: z3.union([z3.literal("fast"), z3.literal("slow")]),
      extra: z3.record(z3.number()),
    }),
  );
  assert.deepEqual(schema.jsonSchema, {
    type: "object",
    properties: {
      query: { type: "string", description: "What to search for" },
      limit: { type: "integer" },
      exact: { type: "boolean", default: false },
      kind: { enum: ["code", "docs"] },
      tags: { type: "array", items: { type: "string" } },
      filter: {
        type: "object",
        properties: { path: { anyOf: [{ type: "string" }, { type: "null" }] } },
        additionalProperties: false,
        required: ["path"],
      },
      mode: { anyOf: [{ const: "fast" }, { const: "slow" }] },
      extra: { type: "object", additionalProperties: { type: "number" } },
    },
    additionalProperties: false,
    required: ["query", "kind", "tags", "filter", "mode", "extra"],
  });
});

test("converts a zod v4 object schema to JSON Schema", () => {
  const schema = resolveSchema(
    z4.object({
      query: z4.string().describe("What to search for"),
      limit: z4.int().optional(),
      exact: z4.boolean().default(false),
      kind: z4.enum(["code", "docs"]),
      tags: z4.array(z4.string()),
      mode: z4.union([z4.literal("fast"), z4.literal("slow")]),
    }),
  );
  assert.deepEqual(schema.jsonSchema, {
    type: "object",
    properties: {
      query: { type: "string", description: "What to search for" },
      limit: { type: "integer" },
      exact: { type: "boolean", default: false },
      kind: { enum: ["code", "docs"] },
      tags: { type: "array", items: { type: "string" } },
      mode: { anyOf: [{ const: "fast" }, { const: "slow" }] },
    },
    additionalProperties: false,
    required: ["query", "kind", "tags", "mode"],
  });
});

test("validates through Standard Schema and applies defaults", async () => {
  const schema = resolveSchema(z3.object({ q: z3.string(), n: z3.number().default(3) }));
  const result = await schema.validate({ q: "hi" });
  assert.deepEqual(result, { success: true, value: { q: "hi", n: 3 } });
});

test("reports validation issues with paths", async () => {
  const schema = resolveSchema(z3.object({ q: z3.string() }));
  const result = await schema.validate({ q: 42 });
  assert.equal(result.success, false);
  if (!result.success) {
    assert.equal(result.issues[0]?.path, "q");
    assert.ok(result.issues[0]!.message.length > 0);
  }
});

test("unsupported zod constructs throw with the jsonSchema() escape hatch named", () => {
  assert.throws(
    () => resolveSchema(z3.object({ m: z3.map(z3.string(), z3.number()) })),
    (error: unknown) => error instanceof AIError && /jsonSchema\(\)/.test(error.message),
  );
});

test("non-zod Standard Schemas are rejected with guidance", () => {
  const fake = {
    "~standard": { version: 1 as const, vendor: "valibot", validate: (value: unknown) => ({ value }) },
  };
  assert.throws(
    () => resolveSchema(fake as never),
    (error: unknown) => error instanceof AIError && /valibot/.test(error.message),
  );
});

test("jsonSchema wraps raw JSON Schema and passes values through", async () => {
  const schema = resolveSchema(jsonSchema<{ q: string }>({ type: "object" }));
  assert.deepEqual(schema.jsonSchema, { type: "object" });
  assert.deepEqual(await schema.validate({ q: "x" }), { success: true, value: { q: "x" } });
});

test("jsonSchema accepts a custom validator", async () => {
  const schema = resolveSchema(
    jsonSchema<number>(
      { type: "integer" },
      { validate: (value) => (Number.isInteger(value) ? { success: true, value: value as number } : { success: false, issues: [{ message: "not an integer" }] }) },
    ),
  );
  assert.deepEqual(await schema.validate(3), { success: true, value: 3 });
  const bad = await schema.validate("x");
  assert.equal(bad.success, false);
});
