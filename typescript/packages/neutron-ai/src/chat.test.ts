import assert from "node:assert/strict";
import { test } from "node:test";

import type { AdapterStreamPart, ModelAdapter } from "./adapter.js";
import { ChatStore } from "./chat-store.js";
import { AIError } from "./errors.js";
import { streamPartsFromResponse, toEventStreamResponse } from "./event-stream.js";
import { streamText } from "./stream-text.js";
import type { Message, StreamPart } from "./types.js";

function streamModel(parts: AdapterStreamPart[], options: { failAfter?: number } = {}): ModelAdapter {
  return {
    provider: "fake",
    modelId: "fake-1",
    async doGenerate() {
      throw new Error("not used");
    },
    async *doStream() {
      let index = 0;
      for (const part of parts) {
        if (options.failAfter !== undefined && index === options.failAfter) throw new Error("upstream failed");
        index += 1;
        yield part;
      }
    },
  };
}

const usage = { inputTokens: 3, outputTokens: 7, totalTokens: 10 };

const PARTS: AdapterStreamPart[] = [
  { type: "text-delta", text: "Hello" },
  { type: "text-delta", text: " world" },
  { type: "finish", finishReason: "stop", usage },
];

test("event-stream response round-trips StreamParts exactly", async () => {
  const result = streamText({ model: streamModel(PARTS), prompt: "hi" });
  const response = toEventStreamResponse(result);
  assert.equal(response.headers.get("content-type"), "text/event-stream");

  const parts: StreamPart[] = [];
  for await (const part of streamPartsFromResponse(response)) parts.push(part);
  assert.deepEqual(parts, [
    { type: "text-delta", text: "Hello" },
    { type: "text-delta", text: " world" },
    { type: "finish", finishReason: "stop", usage },
  ]);
});

test("stream errors cross the wire as AIError with the original problem", async () => {
  const result = streamText({ model: streamModel(PARTS, { failAfter: 1 }), prompt: "hi" });
  const response = toEventStreamResponse(result);
  await assert.rejects(
    (async () => {
      for await (const _part of streamPartsFromResponse(response)) {
        // consume until the error event
      }
    })(),
    (error: unknown) =>
      error instanceof AIError && error.problem.status === 500 && /upstream failed/.test(error.problem.detail),
  );
});

test("ChatStore streams an assistant reply and posts the history", async () => {
  let sentBody: { messages: Message[] } | undefined;
  const fetchImpl = (async (_input: RequestInfo | URL, init?: RequestInit) => {
    sentBody = JSON.parse(init?.body as string) as { messages: Message[] };
    const result = streamText({ model: streamModel(PARTS), prompt: "hi" });
    return toEventStreamResponse(result);
  }) as typeof globalThis.fetch;

  const store = new ChatStore({ api: "/api/chat", fetch: fetchImpl });
  const states: string[] = [];
  store.subscribe(() => states.push(store.getState().status));

  await store.send("hi there");

  assert.deepEqual(sentBody, { messages: [{ role: "user", content: "hi there" }] });
  const { messages, status } = store.getState();
  assert.equal(status, "idle");
  assert.equal(messages.length, 2);
  assert.deepEqual(messages[0], { id: "msg-1", role: "user", content: "hi there" });
  assert.deepEqual(messages[1], { id: "msg-2", role: "assistant", content: "Hello world" });
  assert.ok(states.includes("streaming"));
});

test("ChatStore surfaces wire errors in state instead of throwing", async () => {
  const fetchImpl = (async () => {
    const result = streamText({ model: streamModel(PARTS, { failAfter: 0 }), prompt: "hi" });
    return toEventStreamResponse(result);
  }) as typeof globalThis.fetch;

  const store = new ChatStore({ api: "/api/chat", fetch: fetchImpl });
  await store.send("hi");
  const state = store.getState();
  assert.equal(state.status, "error");
  assert.equal(state.error?.problem.status, 500);
});

test("ChatStore rejects concurrent sends", async () => {
  const fetchImpl = (async () => {
    const result = streamText({ model: streamModel(PARTS), prompt: "hi" });
    return toEventStreamResponse(result);
  }) as typeof globalThis.fetch;
  const store = new ChatStore({ api: "/api/chat", fetch: fetchImpl });
  const first = store.send("one");
  await assert.rejects(
    store.send("two"),
    (error: unknown) => error instanceof AIError && error.problem.status === 409,
  );
  await first;
});

test("non-OK responses become problem-details errors", async () => {
  const response = new Response(
    JSON.stringify({ type: "https://neutron.dev/errors/rate-limited", title: "Rate Limited", status: 429, detail: "slow down" }),
    { status: 429 },
  );
  await assert.rejects(
    (async () => {
      for await (const _part of streamPartsFromResponse(response)) {
        // unreachable
      }
    })(),
    (error: unknown) => error instanceof AIError && error.problem.status === 429 && error.problem.detail === "slow down",
  );
});
