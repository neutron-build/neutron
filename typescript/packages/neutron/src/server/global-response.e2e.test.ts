import * as fs from "node:fs/promises";
import * as net from "node:net";
import * as path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { createServer, type NeutronServer } from "./index.js";

/**
 * Starting the server must not replace `globalThis.Response`/`Request`.
 * `@hono/node-server` swaps both for its own classes at `serve()` unless told
 * not to, and a Response subclass defined before that (a Next-compat
 * `NextResponse` evaluated at import) then fails `instanceof Response` in app
 * code — the Fylun port's `NextResponse.json` answered `200 {}` that way.
 */

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

class AppResponse extends Response {
  static make(body: unknown): AppResponse {
    return new AppResponse(JSON.stringify(body), {
      status: 201,
      headers: { "content-type": "application/json", "x-app-response": "1" },
    });
  }
}

const ROUTE = `
export function GET() {
  const early = globalThis.__neutronEarlyResponse;
  return Response.json({
    earlyIsResponse: early instanceof Response,
    jsonIsResponse: Response.json({}) instanceof Response,
  });
}
export function POST() {
  return globalThis.__neutronAppResponse.make({ made: true });
}
`;

describe("global Response at server start", () => {
  let running: NeutronServer | null = null;
  let root = "";

  afterEach(async () => {
    if (running) {
      await running.close();
      running = null;
    }
    if (root) {
      await fs.rm(root, { recursive: true, force: true });
      root = "";
    }
    delete (globalThis as Record<string, unknown>).__neutronEarlyResponse;
    delete (globalThis as Record<string, unknown>).__neutronAppResponse;
  });

  it(
    "keeps the native Response/Request, so pre-start subclasses stay instanceof Response",
    { timeout: 30_000 },
    async () => {
      const NativeResponse = globalThis.Response;
      const NativeRequest = globalThis.Request;
      const early = AppResponse.make({ early: true });
      (globalThis as Record<string, unknown>).__neutronEarlyResponse = early;
      (globalThis as Record<string, unknown>).__neutronAppResponse = AppResponse;

      root = await fs.mkdtemp(path.join(process.cwd(), ".tmp-neutron-global-response-"));
      await fs.mkdir(path.join(root, "src", "routes", "api"), { recursive: true });
      await fs.mkdir(path.join(root, "dist"), { recursive: true });
      await fs.writeFile(path.join(root, "src", "routes", "api", "probe.ts"), ROUTE, "utf-8");
      const port = await getFreePort();
      running = await createServer({ host: "127.0.0.1", port, rootDir: root, compress: false });
      const base = `http://127.0.0.1:${port}`;

      expect(globalThis.Response).toBe(NativeResponse);
      expect(globalThis.Request).toBe(NativeRequest);
      expect(early instanceof Response).toBe(true);

      const probe = await fetch(`${base}/api/probe`);
      expect(probe.status).toBe(200);
      expect(await probe.json()).toEqual({ earlyIsResponse: true, jsonIsResponse: true });

      // A subclass instance returned by a handler answers as itself.
      const made = await fetch(`${base}/api/probe`, {
        method: "POST",
        headers: { origin: base },
      });
      expect(made.status).toBe(201);
      expect(made.headers.get("x-app-response")).toBe("1");
      expect(await made.json()).toEqual({ made: true });
    }
  );
});
