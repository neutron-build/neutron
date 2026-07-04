import assert from "node:assert/strict";
import { test } from "node:test";

import { AIError } from "../errors.js";
import { generateText } from "../generate-text.js";
import { jsonSchema } from "../schema.js";
import { streamText } from "../stream-text.js";
import type { StreamPart } from "../types.js";
import { createOpenAI } from "./index.js";

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

function sseResponse(datas: unknown[], chunkSize = 7): Response {
  const raw = datas
    .map((data) => `data: ${typeof data === "string" ? data : JSON.stringify(data)}\n\n`)
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
  id: "chatcmpl-1",
  choices: [
    {
      index: 0,
      message: {
        role: "assistant",
        content: "Searching now.",
        tool_calls: [
          { id: "call_1", type: "function", function: { name: "search", arguments: '{"query":"neutron"}' } },
        ],
      },
      finish_reason: "tool_calls",
    },
  ],
  usage: { prompt_tokens: 25, completion_tokens: 17 },
};

test("doGenerate sends the mapped request body and headers", async () => {
  const { impl, calls } = mockFetch(() => jsonResponse(GENERATE_FIXTURE));
  const openai = createOpenAI({ apiKey: "test-key", fetch: impl });

  await generateText({
    model: openai("gpt-4o"),
    system: "be helpful",
    messages: [
      { role: "user", content: "find neutron" },
      {
        role: "assistant",
        content: [
          { type: "text", text: "checking" },
          { type: "tool-call", toolCallId: "call_0", toolName: "search", input: { query: "old" } },
        ],
      },
      {
        role: "tool",
        content: [{ type: "tool-result", toolCallId: "call_0", toolName: "search", output: { hits: 3 } }],
      },
    ],
    tools: [{ name: "search", description: "Search things", inputSchema: jsonSchema({ type: "object" }) }],
    toolChoice: "auto",
    maxOutputTokens: 1000,
    temperature: 0.5,
  });

  assert.equal(calls[0]?.url, "https://api.openai.com/v1/chat/completions");
  const headers = calls[0]?.init.headers as Record<string, string>;
  assert.equal(headers.authorization, "Bearer test-key");

  const body = JSON.parse(calls[0]?.init.body as string);
  assert.equal(body.model, "gpt-4o");
  assert.equal(body.max_tokens, 1000);
  assert.equal(body.temperature, 0.5);
  assert.equal(body.tool_choice, "auto");
  assert.deepEqual(body.tools, [
    { type: "function", function: { name: "search", description: "Search things", parameters: { type: "object" } } },
  ]);
  assert.deepEqual(body.messages, [
    { role: "system", content: "be helpful" },
    { role: "user", content: "find neutron" },
    {
      role: "assistant",
      content: "checking",
      tool_calls: [
        { id: "call_0", type: "function", function: { name: "search", arguments: '{"query":"old"}' } },
      ],
    },
    { role: "tool", tool_call_id: "call_0", content: '{"hits":3}' },
  ]);
});

test("doGenerate parses text, tool calls, finish reason, and usage", async () => {
  const { impl } = mockFetch(() => jsonResponse(GENERATE_FIXTURE));
  const openai = createOpenAI({ apiKey: "test-key", fetch: impl });
  const result = await generateText({ model: openai("gpt-4o"), prompt: "find neutron" });

  assert.equal(result.text, "Searching now.");
  assert.deepEqual(result.toolCalls, [
    { type: "tool-call", toolCallId: "call_1", toolName: "search", input: { query: "neutron" } },
  ]);
  assert.equal(result.finishReason, "tool-calls");
  assert.deepEqual(result.usage, { inputTokens: 25, outputTokens: 17, totalTokens: 42 });
});

test("streaming reassembles split tool-call arguments and reads the usage chunk", async () => {
  const { impl, calls } = mockFetch(() =>
    sseResponse([
      { choices: [{ delta: { role: "assistant", content: "" } }] },
      { choices: [{ delta: { content: "Hello" } }] },
      { choices: [{ delta: { content: " world" } }] },
      {
        choices: [
          { delta: { tool_calls: [{ index: 0, id: "call_1", function: { name: "search", arguments: '{"que' } }] } },
        ],
      },
      { choices: [{ delta: { tool_calls: [{ index: 0, function: { arguments: 'ry":"neutron"}' } }] } }] },
      { choices: [{ delta: {}, finish_reason: "tool_calls" }] },
      { choices: [], usage: { prompt_tokens: 25, completion_tokens: 17 } },
      "[DONE]",
    ]),
  );
  const openai = createOpenAI({ apiKey: "test-key", fetch: impl });
  const result = streamText({ model: openai("gpt-4o"), prompt: "find neutron" });
  const parts: StreamPart[] = [];
  for await (const part of result.fullStream) parts.push(part);

  assert.deepEqual(parts, [
    { type: "text-delta", text: "Hello" },
    { type: "text-delta", text: " world" },
    { type: "tool-input-start", toolCallId: "call_1", toolName: "search" },
    { type: "tool-input-delta", toolCallId: "call_1", delta: '{"que' },
    { type: "tool-input-delta", toolCallId: "call_1", delta: 'ry":"neutron"}' },
    { type: "tool-call", toolCallId: "call_1", toolName: "search", input: { query: "neutron" } },
    { type: "finish", finishReason: "tool-calls", usage: { inputTokens: 25, outputTokens: 17, totalTokens: 42 } },
  ]);
  const body = JSON.parse(calls[0]?.init.body as string);
  assert.equal(body.stream, true);
  assert.deepEqual(body.stream_options, { include_usage: true });
});

test("HTTP errors map to problem details with the provider message", async () => {
  const { impl } = mockFetch(() =>
    jsonResponse({ error: { message: "Rate limit reached", type: "tokens" } }, 429),
  );
  const openai = createOpenAI({ apiKey: "test-key", fetch: impl });
  await assert.rejects(
    generateText({ model: openai("gpt-4o"), prompt: "hi", maxRetries: 0 }),
    (error: unknown) =>
      error instanceof AIError &&
      error.problem.status === 429 &&
      error.problem.detail === "Rate limit reached",
  );
});

test("automatic caching surfaces cached tokens (OpenAI details field and DeepSeek's)", async () => {
  const withDetails = {
    ...GENERATE_FIXTURE,
    usage: { prompt_tokens: 2000, completion_tokens: 10, prompt_tokens_details: { cached_tokens: 1792 } },
  };
  const first = mockFetch(() => jsonResponse(withDetails));
  const openaiP = createOpenAI({ apiKey: "k", fetch: first.impl });
  const a = await generateText({ model: openaiP("gpt-4o"), prompt: "hi" });
  assert.equal(a.usage.cacheReadTokens, 1792);
  // cached tokens are INCLUDED in prompt_tokens on this wire — total unchanged
  assert.equal(a.usage.totalTokens, 2010);

  const deepseekUsage = { ...GENERATE_FIXTURE, usage: { prompt_tokens: 500, completion_tokens: 5, prompt_cache_hit_tokens: 400 } };
  const second = mockFetch(() => jsonResponse(deepseekUsage));
  const deepseek = createOpenAI({ apiKey: "k", baseURL: "https://api.deepseek.com", provider: "deepseek", fetch: second.impl });
  const b = await generateText({ model: deepseek("deepseek-chat"), prompt: "hi" });
  assert.equal(b.usage.cacheReadTokens, 400);
});

test("a custom baseURL and provider label serve OpenAI-compatible servers", async () => {
  const { impl, calls } = mockFetch(() => jsonResponse(GENERATE_FIXTURE));
  const groq = createOpenAI({
    apiKey: "test-key",
    baseURL: "https://api.groq.com/openai",
    provider: "groq",
    fetch: impl,
  });
  const model = groq("llama-3.3-70b");
  assert.equal(model.provider, "groq");
  await generateText({ model, prompt: "hi" });
  assert.equal(calls[0]?.url, "https://api.groq.com/openai/v1/chat/completions");
});

test("a missing API key throws unauthorized without calling fetch", async () => {
  const { impl, calls } = mockFetch(() => jsonResponse(GENERATE_FIXTURE));
  const openai = createOpenAI({ fetch: impl });
  const saved = process.env.OPENAI_API_KEY;
  delete process.env.OPENAI_API_KEY;
  try {
    await assert.rejects(
      generateText({ model: openai("gpt-4o"), prompt: "hi" }),
      (error: unknown) => error instanceof AIError && error.problem.status === 401,
    );
    assert.equal(calls.length, 0);
  } finally {
    if (saved !== undefined) process.env.OPENAI_API_KEY = saved;
  }
});
