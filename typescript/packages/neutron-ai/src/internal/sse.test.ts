import assert from "node:assert/strict";
import { test } from "node:test";

import { parseSSE, type SSEEvent } from "./sse.js";

function streamFromChunks(chunks: string[]): ReadableStream<Uint8Array> {
  const encoder = new TextEncoder();
  return new ReadableStream({
    start(controller) {
      for (const chunk of chunks) controller.enqueue(encoder.encode(chunk));
      controller.close();
    },
  });
}

async function collect(body: ReadableStream<Uint8Array>): Promise<SSEEvent[]> {
  const events: SSEEvent[] = [];
  for await (const event of parseSSE(body)) events.push(event);
  return events;
}

test("parses LF-delimited events", async () => {
  const events = await collect(streamFromChunks(['event: ping\ndata: {"a":1}\n\ndata: solo\n\n']));
  assert.deepEqual(events, [
    { event: "ping", data: '{"a":1}' },
    { data: "solo" },
  ]);
});

test("parses CRLF-delimited events", async () => {
  const events = await collect(streamFromChunks(["event: x\r\ndata: y\r\n\r\n"]));
  assert.deepEqual(events, [{ event: "x", data: "y" }]);
});

test("joins multi-line data with newlines", async () => {
  const events = await collect(streamFromChunks(["data: line1\ndata: line2\n\n"]));
  assert.deepEqual(events, [{ data: "line1\nline2" }]);
});

test("ignores comment lines and unknown fields", async () => {
  const events = await collect(streamFromChunks([": keep-alive\nretry: 500\ndata: ok\n\n"]));
  assert.deepEqual(events, [{ data: "ok" }]);
});

test("reassembles events split across arbitrary chunk boundaries", async () => {
  const raw = 'event: message\ndata: {"text":"hello world"}\n\nevent: done\ndata: {}\n\n';
  const chunks: string[] = [];
  for (let i = 0; i < raw.length; i += 3) chunks.push(raw.slice(i, i + 3));
  const events = await collect(streamFromChunks(chunks));
  assert.deepEqual(events, [
    { event: "message", data: '{"text":"hello world"}' },
    { event: "done", data: "{}" },
  ]);
});

test("emits a trailing event missing its final delimiter", async () => {
  const events = await collect(streamFromChunks(["data: tail"]));
  assert.deepEqual(events, [{ data: "tail" }]);
});
