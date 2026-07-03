import assert from "node:assert/strict";
import { test } from "node:test";

import { embed, embedAndStore, embedMany, type VectorSink } from "./embed.js";
import { createOpenAI } from "./openai/index.js";

function mockFetch(handler: (url: string, init: RequestInit) => Response): {
  impl: typeof globalThis.fetch;
  calls: Array<{ url: string; init: RequestInit }>;
} {
  const calls: Array<{ url: string; init: RequestInit }> = [];
  const impl = (async (input: RequestInfo | URL, init?: RequestInit) => {
    const url =
      typeof input === "string" ? input : input instanceof URL ? input.toString() : input.url;
    calls.push({ url, init: init ?? {} });
    return handler(url, init ?? {});
  }) as typeof globalThis.fetch;
  return { impl, calls };
}

const EMBEDDINGS_FIXTURE = {
  data: [
    { index: 1, embedding: [0.4, 0.5] },
    { index: 0, embedding: [0.1, 0.2] },
  ],
  usage: { prompt_tokens: 7 },
};

test("embedMany hits /v1/embeddings and restores input order", async () => {
  const { impl, calls } = mockFetch(() => new Response(JSON.stringify(EMBEDDINGS_FIXTURE), { status: 200 }));
  const openai = createOpenAI({ apiKey: "test-key", fetch: impl });
  const result = await embedMany({
    model: openai.embedding("text-embedding-3-small"),
    values: ["alpha", "beta"],
  });

  assert.equal(calls[0]?.url, "https://api.openai.com/v1/embeddings");
  const body = JSON.parse(calls[0]?.init.body as string);
  assert.equal(body.model, "text-embedding-3-small");
  assert.deepEqual(body.input, ["alpha", "beta"]);

  assert.deepEqual(result.embeddings, [
    [0.1, 0.2],
    [0.4, 0.5],
  ]);
  assert.deepEqual(result.usage, { inputTokens: 7 });
});

test("embed returns the single embedding", async () => {
  const { impl } = mockFetch(
    () => new Response(JSON.stringify({ data: [{ index: 0, embedding: [1, 2] }], usage: { prompt_tokens: 2 } }), { status: 200 }),
  );
  const openai = createOpenAI({ apiKey: "test-key", fetch: impl });
  const result = await embed({ model: openai.embedding("text-embedding-3-small"), value: "hello" });
  assert.deepEqual(result.embedding, [1, 2]);
});

test("embedMany with no values short-circuits without a request", async () => {
  const { impl, calls } = mockFetch(() => new Response("{}", { status: 200 }));
  const openai = createOpenAI({ apiKey: "test-key", fetch: impl });
  const result = await embedMany({ model: openai.embedding("text-embedding-3-small"), values: [] });
  assert.deepEqual(result, { embeddings: [], usage: { inputTokens: 0 } });
  assert.equal(calls.length, 0);
});

test("embedAndStore writes each embedding to the vector sink", async () => {
  const { impl } = mockFetch(() => new Response(JSON.stringify(EMBEDDINGS_FIXTURE), { status: 200 }));
  const openai = createOpenAI({ apiKey: "test-key", fetch: impl });

  const inserts: Array<{ collection: string; id: string; vector: number[]; metadata?: Record<string, unknown> }> = [];
  const sink: VectorSink = {
    async insert(collection, id, vector, metadata) {
      inserts.push({ collection, id, vector, metadata: metadata ?? {} });
    },
  };

  const result = await embedAndStore({
    model: openai.embedding("text-embedding-3-small"),
    values: ["alpha", "beta"],
    vector: sink,
    collection: "docs",
    ids: ["a", "b"],
    metadata: (value, index) => ({ value, index }),
  });

  assert.deepEqual(result.ids, ["a", "b"]);
  assert.deepEqual(inserts, [
    { collection: "docs", id: "a", vector: [0.1, 0.2], metadata: { value: "alpha", index: 0 } },
    { collection: "docs", id: "b", vector: [0.4, 0.5], metadata: { value: "beta", index: 1 } },
  ]);
});
