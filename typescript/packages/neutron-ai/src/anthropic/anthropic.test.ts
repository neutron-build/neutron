import assert from "node:assert/strict";
import { test } from "node:test";

import { AIError } from "../errors.js";
import { generateText } from "../generate-text.js";
import { jsonSchema } from "../schema.js";
import { streamText } from "../stream-text.js";
import type { StreamPart } from "../types.js";
import { createAnthropic } from "./index.js";

function mockFetch(handler: (url: string, init: RequestInit) => Response | Promise<Response>): {
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

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}

function sseResponse(events: Array<[string, unknown]>, chunkSize = 7): Response {
  const raw = events
    .map(([name, payload]) => `event: ${name}\ndata: ${JSON.stringify(payload)}\n\n`)
    .join("");
  const bytes = new TextEncoder().encode(raw);
  const stream = new ReadableStream<Uint8Array>({
    start(controller) {
      for (let i = 0; i < bytes.length; i += chunkSize) {
        controller.enqueue(bytes.slice(i, i + chunkSize));
      }
      controller.close();
    },
  });
  return new Response(stream, { status: 200, headers: { "content-type": "text/event-stream" } });
}

const GENERATE_FIXTURE = {
  id: "msg_01",
  type: "message",
  role: "assistant",
  model: "claude-sonnet-5",
  content: [
    { type: "text", text: "Searching now." },
    { type: "tool_use", id: "toolu_01", name: "search", input: { query: "neutron" } },
  ],
  stop_reason: "tool_use",
  usage: { input_tokens: 25, output_tokens: 17 },
};

test("doGenerate sends the mapped request body and headers", async () => {
  const { impl, calls } = mockFetch(() => jsonResponse(GENERATE_FIXTURE));
  const anthropic = createAnthropic({ apiKey: "test-key", fetch: impl });

  await generateText({
    model: anthropic("claude-sonnet-5"),
    system: "be helpful",
    messages: [
      { role: "user", content: "find neutron" },
      {
        role: "assistant",
        content: [{ type: "tool-call", toolCallId: "toolu_00", toolName: "search", input: { query: "old" } }],
      },
      {
        role: "tool",
        content: [{ type: "tool-result", toolCallId: "toolu_00", toolName: "search", output: { hits: 3 } }],
      },
    ],
    tools: [{ name: "search", description: "Search things", inputSchema: jsonSchema({ type: "object" }) }],
    maxOutputTokens: 1000,
    temperature: 0.5,
    topP: 0.9,
    stopSequences: ["END"],
  });

  assert.equal(calls.length, 1);
  assert.equal(calls[0]?.url, "https://api.anthropic.com/v1/messages");
  const headers = calls[0]?.init.headers as Record<string, string>;
  assert.equal(headers["x-api-key"], "test-key");
  assert.equal(headers["anthropic-version"], "2023-06-01");

  const body = JSON.parse(calls[0]?.init.body as string);
  assert.equal(body.model, "claude-sonnet-5");
  assert.equal(body.max_tokens, 1000);
  assert.equal(body.system, "be helpful");
  assert.equal(body.temperature, 0.5);
  assert.equal(body.top_p, 0.9);
  assert.deepEqual(body.stop_sequences, ["END"]);
  assert.deepEqual(body.tools, [
    { name: "search", description: "Search things", input_schema: { type: "object" } },
  ]);
  assert.deepEqual(body.messages, [
    { role: "user", content: [{ type: "text", text: "find neutron" }] },
    {
      role: "assistant",
      content: [{ type: "tool_use", id: "toolu_00", name: "search", input: { query: "old" } }],
    },
    {
      role: "user",
      content: [{ type: "tool_result", tool_use_id: "toolu_00", content: '{"hits":3}' }],
    },
  ]);
});

test("max_tokens defaults when not specified", async () => {
  const { impl, calls } = mockFetch(() => jsonResponse(GENERATE_FIXTURE));
  const anthropic = createAnthropic({ apiKey: "test-key", fetch: impl });
  await generateText({ model: anthropic("claude-sonnet-5"), prompt: "hi" });
  const body = JSON.parse(calls[0]?.init.body as string);
  assert.equal(body.max_tokens, 4096);
});

test("doGenerate parses text, tool calls, finish reason, and usage", async () => {
  const { impl } = mockFetch(() => jsonResponse(GENERATE_FIXTURE));
  const anthropic = createAnthropic({ apiKey: "test-key", fetch: impl });
  const result = await generateText({ model: anthropic("claude-sonnet-5"), prompt: "find neutron" });

  assert.equal(result.text, "Searching now.");
  assert.deepEqual(result.toolCalls, [
    { type: "tool-call", toolCallId: "toolu_01", toolName: "search", input: { query: "neutron" } },
  ]);
  assert.equal(result.finishReason, "tool-calls");
  assert.deepEqual(result.usage, { inputTokens: 25, outputTokens: 17, totalTokens: 42 });
});

test("streaming assembles text deltas and split tool-call input JSON", async () => {
  const events: Array<[string, unknown]> = [
    ["message_start", {
      type: "message_start",
      message: { id: "msg_01", type: "message", role: "assistant", content: [], usage: { input_tokens: 25, output_tokens: 1 } },
    }],
    ["content_block_start", { type: "content_block_start", index: 0, content_block: { type: "text", text: "" } }],
    ["ping", { type: "ping" }],
    ["content_block_delta", { type: "content_block_delta", index: 0, delta: { type: "text_delta", text: "Hello" } }],
    ["content_block_delta", { type: "content_block_delta", index: 0, delta: { type: "text_delta", text: " world" } }],
    ["content_block_stop", { type: "content_block_stop", index: 0 }],
    ["content_block_start", { type: "content_block_start", index: 1, content_block: { type: "tool_use", id: "toolu_01", name: "search", input: {} } }],
    ["content_block_delta", { type: "content_block_delta", index: 1, delta: { type: "input_json_delta", partial_json: '{"query":' } }],
    ["content_block_delta", { type: "content_block_delta", index: 1, delta: { type: "input_json_delta", partial_json: '"neutron"}' } }],
    ["content_block_stop", { type: "content_block_stop", index: 1 }],
    ["message_delta", { type: "message_delta", delta: { stop_reason: "tool_use", stop_sequence: null }, usage: { output_tokens: 17 } }],
    ["message_stop", { type: "message_stop" }],
  ];
  const { impl } = mockFetch(() => sseResponse(events));
  const anthropic = createAnthropic({ apiKey: "test-key", fetch: impl });

  const result = streamText({ model: anthropic("claude-sonnet-5"), prompt: "find neutron" });
  const parts: StreamPart[] = [];
  for await (const part of result.fullStream) parts.push(part);

  assert.deepEqual(parts, [
    { type: "text-delta", text: "Hello" },
    { type: "text-delta", text: " world" },
    { type: "tool-input-start", toolCallId: "toolu_01", toolName: "search" },
    { type: "tool-input-delta", toolCallId: "toolu_01", delta: '{"query":' },
    { type: "tool-input-delta", toolCallId: "toolu_01", delta: '"neutron"}' },
    { type: "tool-call", toolCallId: "toolu_01", toolName: "search", input: { query: "neutron" } },
    { type: "finish", finishReason: "tool-calls", usage: { inputTokens: 25, outputTokens: 17, totalTokens: 42 } },
  ]);
  assert.equal(await result.text, "Hello world");
});

test("streaming requests set stream: true", async () => {
  const { impl, calls } = mockFetch(() => sseResponse([["message_stop", { type: "message_stop" }]]));
  const anthropic = createAnthropic({ apiKey: "test-key", fetch: impl });
  const result = streamText({ model: anthropic("claude-sonnet-5"), prompt: "hi" });
  await result.text;
  const body = JSON.parse(calls[0]?.init.body as string);
  assert.equal(body.stream, true);
});

test("an SSE error event throws a mapped AIError", async () => {
  const events: Array<[string, unknown]> = [
    ["message_start", { type: "message_start", message: { usage: { input_tokens: 1 } } }],
    ["error", { type: "error", error: { type: "overloaded_error", message: "Overloaded" } }],
  ];
  const { impl } = mockFetch(() => sseResponse(events));
  const anthropic = createAnthropic({ apiKey: "test-key", fetch: impl });
  const result = streamText({ model: anthropic("claude-sonnet-5"), prompt: "hi", maxRetries: 0 });
  await assert.rejects(
    result.text,
    (error: unknown) =>
      error instanceof AIError && error.problem.status === 500 && error.message === "Overloaded",
  );
});

test("HTTP 429 maps to rate-limited with the provider message", async () => {
  const { impl } = mockFetch(() =>
    jsonResponse({ type: "error", error: { type: "rate_limit_error", message: "Too many requests" } }, 429),
  );
  const anthropic = createAnthropic({ apiKey: "test-key", fetch: impl });
  await assert.rejects(
    generateText({ model: anthropic("claude-sonnet-5"), prompt: "hi", maxRetries: 0 }),
    (error: unknown) =>
      error instanceof AIError &&
      error.problem.status === 429 &&
      error.problem.type === "https://neutron.dev/errors/rate-limited" &&
      error.problem.detail === "Too many requests",
  );
});

test("provider 5xx maps to internal", async () => {
  const { impl } = mockFetch(() =>
    jsonResponse({ type: "error", error: { type: "overloaded_error", message: "Overloaded" } }, 529),
  );
  const anthropic = createAnthropic({ apiKey: "test-key", fetch: impl });
  await assert.rejects(
    generateText({ model: anthropic("claude-sonnet-5"), prompt: "hi", maxRetries: 0 }),
    (error: unknown) =>
      error instanceof AIError &&
      error.problem.status === 500 &&
      error.problem.type === "https://neutron.dev/errors/internal" &&
      error.problem.detail === "Overloaded",
  );
});

test("a missing API key throws unauthorized without calling fetch", async () => {
  const { impl, calls } = mockFetch(() => jsonResponse(GENERATE_FIXTURE));
  const anthropic = createAnthropic({ fetch: impl });
  const saved = process.env.ANTHROPIC_API_KEY;
  delete process.env.ANTHROPIC_API_KEY;
  try {
    await assert.rejects(
      generateText({ model: anthropic("claude-sonnet-5"), prompt: "hi" }),
      (error: unknown) => error instanceof AIError && error.problem.status === 401,
    );
    assert.equal(calls.length, 0);
  } finally {
    if (saved !== undefined) process.env.ANTHROPIC_API_KEY = saved;
  }
});

test("toolChoice maps to Anthropic tool_choice", async () => {
  const { impl, calls } = mockFetch(() => jsonResponse(GENERATE_FIXTURE));
  const anthropic = createAnthropic({ apiKey: "test-key", fetch: impl });
  const tools = [{ name: "search", inputSchema: jsonSchema({ type: "object" }) }];

  await generateText({ model: anthropic("claude-sonnet-5"), prompt: "hi", tools, toolChoice: "required" });
  assert.deepEqual(JSON.parse(calls[0]?.init.body as string).tool_choice, { type: "any" });

  await generateText({ model: anthropic("claude-sonnet-5"), prompt: "hi", tools, toolChoice: { toolName: "search" } });
  assert.deepEqual(JSON.parse(calls[1]?.init.body as string).tool_choice, { type: "tool", name: "search" });
});

test("consecutive tool messages merge into one user turn (resume shape)", async () => {
  const { impl, calls } = mockFetch(() => jsonResponse(GENERATE_FIXTURE));
  const anthropic = createAnthropic({ apiKey: "test-key", fetch: impl });
  await generateText({
    model: anthropic("claude-sonnet-5"),
    messages: [
      { role: "user", content: "go" },
      {
        role: "assistant",
        content: [
          { type: "tool-call", toolCallId: "a", toolName: "echo", input: {} },
          { type: "tool-call", toolCallId: "b", toolName: "danger", input: {} },
        ],
      },
      { role: "tool", content: [{ type: "tool-result", toolCallId: "a", toolName: "echo", output: "ok" }] },
      { role: "tool", content: [{ type: "tool-result", toolCallId: "b", toolName: "danger", output: "done" }] },
    ],
  });
  const body = JSON.parse(calls[0]?.init.body as string);
  assert.equal(body.messages.length, 3);
  assert.deepEqual(body.messages[2], {
    role: "user",
    content: [
      { type: "tool_result", tool_use_id: "a", content: "ok" },
      { type: "tool_result", tool_use_id: "b", content: "done" },
    ],
  });
});

test("thinking blocks parse to reasoning parts and round-trip with signatures", async () => {
  const fixture = {
    ...GENERATE_FIXTURE,
    content: [
      { type: "thinking", thinking: "Let me consider.", signature: "sig-1" },
      { type: "text", text: "The answer is 4." },
    ],
    stop_reason: "end_turn",
  };
  const { impl, calls } = mockFetch(() => jsonResponse(fixture));
  const anthropic = createAnthropic({ apiKey: "test-key", fetch: impl });

  const result = await generateText({ model: anthropic("claude-sonnet-5"), prompt: "2+2?" });
  assert.equal(result.reasoning, "Let me consider.");
  assert.deepEqual(result.content[0], { type: "reasoning", text: "Let me consider.", signature: "sig-1" });
  assert.equal(result.text, "The answer is 4.");

  // round-trip: signed reasoning and redacted blocks go back; unsigned is dropped
  await generateText({
    model: anthropic("claude-sonnet-5"),
    messages: [
      { role: "user", content: "2+2?" },
      {
        role: "assistant",
        content: [
          { type: "reasoning", text: "Let me consider.", signature: "sig-1" },
          { type: "reasoning", text: "", redactedData: "opaque" },
          { type: "reasoning", text: "unsigned from elsewhere" },
          { type: "text", text: "The answer is 4." },
        ],
      },
      { role: "user", content: "why?" },
    ],
  });
  const body = JSON.parse(calls[1]?.init.body as string);
  assert.deepEqual(body.messages[1].content, [
    { type: "thinking", thinking: "Let me consider.", signature: "sig-1" },
    { type: "redacted_thinking", data: "opaque" },
    { type: "text", text: "The answer is 4." },
  ]);
});

test("the thinking option enables extended thinking on the wire", async () => {
  const { impl, calls } = mockFetch(() => jsonResponse(GENERATE_FIXTURE));
  const anthropic = createAnthropic({ apiKey: "test-key", fetch: impl });
  await generateText({
    model: anthropic("claude-sonnet-5", { thinking: { budgetTokens: 2048 } }),
    prompt: "hi",
    maxOutputTokens: 4096,
  });
  const body = JSON.parse(calls[0]?.init.body as string);
  assert.deepEqual(body.thinking, { type: "enabled", budget_tokens: 2048 });
});

test("streaming thinking emits reasoning deltas and an assembled signed part", async () => {
  const events: Array<[string, unknown]> = [
    ["message_start", { type: "message_start", message: { usage: { input_tokens: 10, output_tokens: 1 } } }],
    ["content_block_start", { type: "content_block_start", index: 0, content_block: { type: "thinking", thinking: "" } }],
    ["content_block_delta", { type: "content_block_delta", index: 0, delta: { type: "thinking_delta", thinking: "Consider " } }],
    ["content_block_delta", { type: "content_block_delta", index: 0, delta: { type: "thinking_delta", thinking: "the input." } }],
    ["content_block_delta", { type: "content_block_delta", index: 0, delta: { type: "signature_delta", signature: "sig-2" } }],
    ["content_block_stop", { type: "content_block_stop", index: 0 }],
    ["content_block_start", { type: "content_block_start", index: 1, content_block: { type: "text", text: "" } }],
    ["content_block_delta", { type: "content_block_delta", index: 1, delta: { type: "text_delta", text: "Done." } }],
    ["content_block_stop", { type: "content_block_stop", index: 1 }],
    ["message_delta", { type: "message_delta", delta: { stop_reason: "end_turn", stop_sequence: null }, usage: { output_tokens: 9 } }],
    ["message_stop", { type: "message_stop" }],
  ];
  const { impl } = mockFetch(() => sseResponse(events));
  const anthropic = createAnthropic({ apiKey: "test-key", fetch: impl });

  const result = streamText({ model: anthropic("claude-sonnet-5"), prompt: "go" });
  const parts: StreamPart[] = [];
  for await (const part of result.fullStream) parts.push(part);

  assert.deepEqual(parts, [
    { type: "reasoning-delta", text: "Consider " },
    { type: "reasoning-delta", text: "the input." },
    { type: "reasoning", text: "Consider the input.", signature: "sig-2" },
    { type: "text-delta", text: "Done." },
    { type: "finish", finishReason: "stop", usage: { inputTokens: 10, outputTokens: 9, totalTokens: 19 } },
  ]);

  // the assistant message reconstructed for continuation carries the signed reasoning first
  const messages = await result.messages;
  const assistant = messages.at(-1) as { content: Array<{ type: string }> };
  assert.deepEqual(assistant.content[0], { type: "reasoning", text: "Consider the input.", signature: "sig-2" });
});

test("a custom baseURL routes the request (AI Gateway seam)", async () => {
  const { impl, calls } = mockFetch(() => jsonResponse(GENERATE_FIXTURE));
  const anthropic = createAnthropic({ apiKey: "test-key", baseURL: "https://gateway.example.com/", fetch: impl });
  await generateText({ model: anthropic("claude-sonnet-5"), prompt: "hi" });
  assert.equal(calls[0]?.url, "https://gateway.example.com/v1/messages");
});
