import * as fs from "node:fs/promises";
import * as http from "node:http";
import * as os from "node:os";
import * as path from "node:path";
import * as zlib from "node:zlib";
import { Hono } from "hono";
import { afterAll, describe, expect, it } from "vitest";
import { createServer } from "./index.js";

/**
 * Dynamic compression (server/compression.ts): complete bodies are
 * compressed in one call with a Content-Length, small ones are left alone,
 * and streamed bodies still reach the client chunk by chunk.
 */

const closers: Array<() => Promise<void>> = [];
afterAll(async () => {
  await Promise.all(closers.map((close) => close()));
});

const LARGE = JSON.stringify(Array.from({ length: 200 }, (_, i) => ({ id: i, title: `Title ${i}` })));

function deferred(): { promise: Promise<void>; resolve: () => void } {
  let resolve!: () => void;
  const promise = new Promise<void>((r) => (resolve = r));
  return { promise, resolve };
}

async function boot(sub: Hono, compress = true): Promise<number> {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "neutron-compression-"));
  const running = await createServer({ mode: "api", rootDir: root, host: "127.0.0.1", port: 0, compress, mount: { "/t": sub } });
  closers.push(async () => {
    await running.close();
    await fs.rm(root, { recursive: true, force: true });
  });
  return Number(new URL(running.url).port);
}

function get(
  port: number,
  pathname: string,
  headers: Record<string, string>,
  onResponse?: (res: http.IncomingMessage) => void
): Promise<{ status: number; headers: http.IncomingHttpHeaders; body: Buffer }> {
  return new Promise((resolve, reject) => {
    const req = http.get({ host: "127.0.0.1", port, path: pathname, headers }, (res) => {
      onResponse?.(res);
      const chunks: Buffer[] = [];
      res.on("data", (chunk: Buffer) => chunks.push(chunk));
      res.on("end", () => resolve({ status: res.statusCode ?? 0, headers: res.headers, body: Buffer.concat(chunks) }));
      res.on("error", reject);
    });
    req.on("error", reject);
  });
}

describe("dynamic compression", () => {
  it("compresses a complete body once, with its length, br preferred", async () => {
    const sub = new Hono();
    sub.get("/json", () => new Response(LARGE, { headers: { "content-type": "application/json", etag: '"v1"' } }));
    const port = await boot(sub);

    const br = await get(port, "/t/json", { "accept-encoding": "gzip, br" });
    expect(br.headers["content-encoding"]).toBe("br");
    expect(Number(br.headers["content-length"])).toBe(br.body.length);
    expect(zlib.brotliDecompressSync(br.body).toString()).toBe(LARGE);
    expect(br.headers.etag).toBe('W/"v1"');
    expect(String(br.headers.vary).toLowerCase()).toContain("accept-encoding");

    const gzip = await get(port, "/t/json", { "accept-encoding": "gzip" });
    expect(gzip.headers["content-encoding"]).toBe("gzip");
    expect(zlib.gunzipSync(gzip.body).toString()).toBe(LARGE);

    const identity = await get(port, "/t/json", { "accept-encoding": "identity" });
    expect(identity.headers["content-encoding"]).toBeUndefined();
    expect(identity.body.toString()).toBe(LARGE);
  });

  it("leaves small, no-transform, binary and compress: false responses alone", async () => {
    const sub = new Hono();
    sub.get("/small", () => new Response('{"ok":true}', { headers: { "content-type": "application/json" } }));
    sub.get("/no-transform", () =>
      new Response(LARGE, { headers: { "content-type": "application/json", "cache-control": "no-transform" } })
    );
    sub.get("/png", () => new Response(new Uint8Array(4096), { headers: { "content-type": "image/png" } }));
    sub.get("/json", () => new Response(LARGE, { headers: { "content-type": "application/json" } }));
    const port = await boot(sub);
    const accept = { "accept-encoding": "br, gzip" };

    const small = await get(port, "/t/small", accept);
    expect(small.headers["content-encoding"]).toBeUndefined();
    expect(small.body.toString()).toBe('{"ok":true}');
    expect(small.headers["content-length"]).toBe("11");
    expect((await get(port, "/t/no-transform", accept)).headers["content-encoding"]).toBeUndefined();
    expect((await get(port, "/t/png", accept)).headers["content-encoding"]).toBeUndefined();

    const off = await boot(sub, false);
    const plain = await get(off, "/t/json", accept);
    expect(plain.headers["content-encoding"]).toBeUndefined();
    expect(plain.body.toString()).toBe(LARGE);
  });

  it("delivers SSE events before the stream ends", async () => {
    const received = deferred();
    const sub = new Hono();
    sub.get("/events", () => {
      const encoder = new TextEncoder();
      const body = new ReadableStream<Uint8Array>({
        async start(controller) {
          controller.enqueue(encoder.encode(`data: ${"first ".repeat(400)}\n\n`));
          await received.promise;
          controller.enqueue(encoder.encode("data: last\n\n"));
          controller.close();
        },
      });
      return new Response(body, { headers: { "content-type": "text/event-stream" } });
    });
    const port = await boot(sub);

    const response = await get(port, "/t/events", { "accept-encoding": "br, gzip" }, (res) => {
      res.once("data", (chunk: Buffer) => {
        expect(chunk.toString()).toContain("data: first");
        received.resolve();
      });
    });
    expect(response.headers["content-encoding"]).toBeUndefined();
    expect(response.body.toString()).toContain("data: last");
  });

  it("compresses a streamed body chunk by chunk, each decodable on arrival", async () => {
    const received = deferred();
    const sub = new Hono();
    sub.get("/stream", () => {
      const encoder = new TextEncoder();
      const body = new ReadableStream<Uint8Array>({
        async start(controller) {
          await new Promise((r) => setTimeout(r, 10));
          controller.enqueue(encoder.encode("<p>shell</p>".repeat(200)));
          await received.promise;
          controller.enqueue(encoder.encode("<p>tail</p>"));
          controller.close();
        },
      });
      return new Response(body, { headers: { "content-type": "text/html; charset=utf-8" } });
    });
    const port = await boot(sub);

    const response = await get(port, "/t/stream", { "accept-encoding": "gzip" }, (res) => {
      expect(res.headers["content-encoding"]).toBe("gzip");
      const gunzip = zlib.createGunzip();
      let decoded = "";
      gunzip.on("data", (chunk: Buffer) => {
        decoded += chunk.toString();
        if (decoded.includes("<p>shell</p>".repeat(200))) received.resolve();
      });
      res.on("data", (chunk: Buffer) => gunzip.write(chunk));
    });
    expect(zlib.gunzipSync(response.body).toString()).toBe("<p>shell</p>".repeat(200) + "<p>tail</p>");
  });
});
