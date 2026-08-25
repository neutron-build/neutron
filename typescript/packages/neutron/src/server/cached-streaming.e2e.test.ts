import * as fs from "node:fs/promises";
import * as net from "node:net";
import * as path from "node:path";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { createServer } from "./index.js";

/**
 * Cached routes (`cache.maxAge`) used to await `response.clone().text()` —
 * the full body drain — before answering. Streaming is defeated exactly on
 * the popular pages: the client sees headers only after the last byte. The
 * cache fill must not gate the response.
 */

let fixtureRoot = "";
let closeServer: (() => Promise<void>) | null = null;
let baseUrl = "";

type Ctrl = ReadableStreamDefaultController<Uint8Array> | null;

async function getFreePort(): Promise<number> {
  return await new Promise<number>((resolve, reject) => {
    const socket = net.createServer();
    socket.listen(0, "127.0.0.1", () => {
      const address = socket.address();
      if (!address || typeof address === "string") {
        reject(new Error("Failed to resolve test port"));
        return;
      }
      const { port } = address;
      socket.close((error) => (error ? reject(error) : resolve(port)));
    });
    socket.on("error", reject);
  });
}

const LIVE_ROUTE = `
import { h } from "preact";
export const config = { mode: "app", cache: { maxAge: 30 } };
export const middleware = async (request, context, next) => {
  // A streaming short-circuit: first chunk now, the rest when the test
  // releases the controller. The client must see headers + the first chunk
  // without waiting for the body to finish.
  const body = new ReadableStream({
    start(controller) {
      const enc = new TextEncoder();
      controller.enqueue(enc.encode("first-chunk"));
      ((globalThis).__liveStreamController = controller);
    },
  });
  return new Response(body, { headers: { "content-type": "text/html; charset=utf-8" } });
};
export default function Page() {
  return h("div", null, "never rendered");
}
`;

function liveController(): Ctrl {
  return ((globalThis as Record<string, unknown>).__liveStreamController as Ctrl) ?? null;
}

beforeAll(async () => {
  fixtureRoot = await fs.mkdtemp(path.join(process.cwd(), ".tmp-neutron-stream-cache-"));
  await fs.mkdir(path.join(fixtureRoot, "src", "routes"), { recursive: true });
  await fs.writeFile(path.join(fixtureRoot, "src", "routes", "live.ts"), LIVE_ROUTE, "utf-8");

  const port = await getFreePort();
  const running = await createServer({
    host: "127.0.0.1",
    port,
    rootDir: fixtureRoot,
    distDir: "dist",
    routesDir: "src/routes",
    compress: false,
  });

  closeServer = running.close;
  baseUrl = `http://127.0.0.1:${port}`;
});

afterAll(async () => {
  try {
    liveController()?.close();
  } catch {
    // already closed
  }
  if (closeServer) {
    await closeServer();
  }
  if (fixtureRoot) {
    await fs.rm(fixtureRoot, { recursive: true, force: true });
  }
});

describe("a cached route answers before its body finishes streaming", () => {
  it("headers and the first chunk arrive without waiting for the full body", { timeout: 20_000 }, async () => {
    const headersPromise = fetch(`${baseUrl}/live`);
    const winner = await Promise.race([
      headersPromise.then((res) => res.status),
      new Promise<never>((_, reject) =>
        setTimeout(() => reject(new Error("headers never arrived — cache fill is gating the response")), 3000)
      ),
    ]);
    expect(winner).toBe(200);

    const res = await headersPromise;
    const reader = res.body!.getReader();
    const { value } = await reader.read();
    expect(new TextDecoder().decode(value)).toBe("first-chunk");

    // The body is still open: the response was handed over mid-stream.
    await reader.cancel().catch(() => {});
    try {
      liveController()?.close();
    } catch {
      // canceled by reader.cancel()
    }
  });
});
