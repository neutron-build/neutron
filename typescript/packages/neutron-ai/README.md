# @neutron-build/ai

AI SDK for Neutron. Model calls, streaming, structured output, and
tool-calling over pluggable provider adapters.

Providers are subpath exports — importing one never loads another, and no
provider SDKs are pulled in at all (adapters speak the provider HTTP APIs
directly over `fetch`).

```ts
import { generateText, streamText } from "@neutron-build/ai";
import { anthropic } from "@neutron-build/ai/anthropic";

const model = anthropic("claude-sonnet-5");

const { text, usage } = await generateText({
  model,
  prompt: "Summarize the Neutron framework in one sentence.",
});

const result = streamText({ model, prompt: "Write a haiku about databases." });
for await (const delta of result.textStream) {
  process.stdout.write(delta);
}
```

## Tools and agents

```ts
import { generateText, tool } from "@neutron-build/ai";
import { z } from "zod";

const search = tool({
  name: "search",
  description: "Search the codebase",
  inputSchema: z.object({ query: z.string() }),
  execute: async ({ query }) => runSearch(query),
});

const deploy = tool({
  name: "deploy",
  inputSchema: z.object({ target: z.string() }),
  execute: async ({ target }) => runDeploy(target),
  needsApproval: true, // or a predicate: ({ target }) => target === "prod"
});

const result = await generateText({ model, prompt, tools: [search, deploy], maxSteps: 5 });

if (result.finishReason === "tool-approval") {
  // Serializable suspension: persist result.messages + result.approvalRequests,
  // then resume — hours or days later — with the decisions:
  await generateText({
    model,
    messages: result.messages,
    tools: [search, deploy],
    maxSteps: 5,
    toolApprovals: [{ toolCallId: result.approvalRequests[0].toolCallId, approved: true }],
  });
}
```

Tool inputs validate through any [Standard Schema](https://standardschema.dev)
library (zod, valibot, arktype). Invalid model inputs, unknown tools, and
tool exceptions become error results fed back to the model so it can
self-correct instead of crashing the loop.

## Structured output

```ts
import { generateObject } from "@neutron-build/ai";

const { object } = await generateObject({
  model,
  prompt: "Extract the review.",
  schema: z.object({ title: z.string(), stars: z.number().int() }),
});
```

Implemented as a forced tool call — the one mechanism every provider
supports identically — so it behaves the same on Anthropic, OpenAI, and
any OpenAI-compatible server.

## Streaming structured output

```ts
import { streamObject } from "@neutron-build/ai";

const result = streamObject({ model, prompt: "Extract the review.", schema });
for await (const partial of result.partialObjectStream) {
  render(partial); // growing snapshots as the JSON streams in
}
const review = await result.object; // final, schema-validated
```

## Chat over the wire

Server (any Neutron `mode:"api"` route — it's just a web-standard Response):

```ts
import { streamText, toEventStreamResponse } from "@neutron-build/ai";

export async function POST(request: Request) {
  const { messages } = await request.json();
  return toEventStreamResponse(streamText({ model, messages, tools, maxSteps: 5 }));
}
```

Client — Preact hook, or the framework-free `ChatStore` it wraps:

```ts
import { useChat } from "@neutron-build/ai/preact";

const { messages, status, send, stop } = useChat({ api: "/api/chat" });
```

## Embeddings

```ts
import { embedMany, embedAndStore } from "@neutron-build/ai";
import { createOpenAI } from "@neutron-build/ai/openai";

const openai = createOpenAI();
const embedder = openai.embedding("text-embedding-3-small");

const { embeddings } = await embedMany({ model: embedder, values: chunks });

// Or write straight to a Nucleus Vector collection:
await embedAndStore({ model: embedder, values: chunks, vector: nucleus.vector, collection: "docs" });
```

## Agent harnesses

One interface for driving any agent — in-process, Claude Code, or your
own — so consumers never couple to a specific one:

```ts
import { localAgent, claudeCode } from "@neutron-build/ai/harness";

const agent = localAgent({ model, tools, maxSteps: 8 });   // this SDK's own loop
// const agent = claudeCode({ permissionMode: "acceptEdits" });  // or the Claude Code CLI

const run = agent.run({ prompt: "fix the failing test", cwd: "/repo" });
for await (const event of run.events) {
  // session | text-delta | tool-start | tool-end | approval-request | finish
}
const { status, output, sessionId, usage, costUSD } = await run.result;
// continue the conversation later:
agent.run({ prompt: "now add a regression test", sessionId });
```

Runs never throw from the event stream — failures arrive as a `finish`
event plus `result.error` (RFC 7807), so every harness fails identically.
`localAgent` supports the SDK's approval suspension (`status:
"suspended"`, resume with `toolApprovals`); CLI harnesses govern
permissions their own way (`permissionMode` for Claude Code).

## Exports

| Subpath | Contents |
|---------|----------|
| `.` | `generateText`, `streamText`, `generateObject`, `streamObject`, `embed`/`embedMany`/`embedAndStore`, `tool`, `jsonSchema`, `ChatStore`, `toEventStreamResponse`/`streamPartsFromResponse`, core types, `ModelAdapter` interface |
| `./anthropic` | `anthropic()` / `createAnthropic()` — Anthropic Messages API adapter |
| `./openai` | `openai()` / `createOpenAI()` — OpenAI Chat Completions + embeddings; via `baseURL` also Groq, DeepSeek, Gemini's OpenAI-compatible endpoint, vLLM/Ollama |
| `./preact` | `useChat` hook (optional `preact` peer dependency) |
| `./harness` | `AgentHarness` interface, `localAgent()`, `claudeCode()` (Node-only subpath) |

## Configuration

The Anthropic adapter reads `ANTHROPIC_API_KEY`, or takes an explicit key:

```ts
import { createAnthropic } from "@neutron-build/ai/anthropic";

const anthropic = createAnthropic({
  apiKey: process.env.MY_KEY,
  baseURL: "https://gateway.example.com", // optional: AI Gateway / proxy
});
```

## Errors

All errors are `AIError` carrying an RFC 7807 problem-details object
(`error.problem`), mapped from provider responses per the framework
contract (429 becomes `rate-limited`, provider 5xx becomes `internal` with
the provider message in `detail`).

## Status

All five milestones complete: core types, `ModelAdapter` interface,
Anthropic + OpenAI adapters, the multi-step tool loop with approval
suspension, `generateObject`/`streamObject`, embeddings with Nucleus
Vector write-through, the event-stream chat wire, the `/preact` hook,
and the `/harness` agent interface with `localAgent` and `claudeCode`
implementations. Also: extended-thinking passthrough (reasoning parts
round-trip with signatures; enable via
`anthropic(model, { thinking: { budgetTokens } })`) and automatic
retries (`maxRetries`, default 2, jittered backoff on 429/5xx; streams
retry only before producing output). A Codex CLI harness follows the
same interface when wire fixtures are captured.
