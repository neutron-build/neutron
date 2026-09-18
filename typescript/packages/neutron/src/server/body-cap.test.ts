// TS-22 actual-byte half: the server adapter enforces the request-body cap
// on REAL bytes as they stream in — covering chunked requests with no
// Content-Length and requests whose declared length lies — while the
// Content-Length early check (middleware + adapter) keeps rejecting
// honestly-declared oversize bodies before any byte is read.
import * as fs from "node:fs/promises";
import * as http from "node:http";
import * as net from "node:net";
import * as path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { createServer, type NeutronServer } from "./index.js";

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

/** POST `body` with the given headers; no Content-Length is set unless
 *  explicitly provided, so the request frames chunked by default. */
function post(
  port: number,
  body: string,
  headers: Record<string, string> = {},
): Promise<{ status: number; text: string }> {
  return new Promise((resolve, reject) => {
    const req = http.request(
      {
        host: "127.0.0.1",
        port,
        method: "POST",
        path: "/echo",
        headers,
      },
      (res) => {
        const chunks: Buffer[] = [];
        res.on("data", (c) => chunks.push(c));
        res.on("end", () =>
          resolve({ status: res.statusCode ?? 0, text: Buffer.concat(chunks).toString("utf8") }),
        );
      },
    );
    req.on("error", reject);
    req.end(body);
  });
}

describe("adapter request-body cap (TS-22)", () => {
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
  });

  async function boot(maxRequestBodyBytes: number): Promise<number> {
    root = await fs.mkdtemp(path.join(process.cwd(), ".tmp-neutron-bodycap-"));
    const port = await getFreePort();
    running = await createServer({
      mode: "raw",
      host: "127.0.0.1",
      port,
      rootDir: root,
      compress: false,
      maxRequestBodyBytes,
    });
    running.app.post("/echo", async (c) => {
      const body = await c.req.text();
      return c.text(`received ${body.length}`);
    });
    return port;
  }

  it("a chunked body with no Content-Length is capped on actual bytes", async () => {
    const port = await boot(1024);
    // Over the cap with NO declared length: the middleware's early check
    // cannot see this one — only the adapter's stream cap can.
    const over = await post(port, "x".repeat(2048));
    expect(over.status).toBe(413);
    // Under the cap passes through intact.
    const under = await post(port, "x".repeat(100));
    expect(under.status).toBe(200);
    expect(under.text).toBe("received 100");
  });

  it("an honestly-declared oversize Content-Length is still rejected early", async () => {
    const port = await boot(1024);
    const res = await post(port, "x".repeat(2048), { "content-length": "2048" });
    expect(res.status).toBe(413);
  });

  it("a lying Content-Length never reaches the handler as a body", async () => {
    const port = await boot(1024);
    // Declares 10 bytes but streams 2048: the early check waves it through;
    // Node's own HTTP framing rejects the length mismatch outright (400)
    // before the extra bytes parse, and the adapter's stream cap is the
    // backstop behind it. Either way the handler must not see a 200.
    const res = await post(port, "x".repeat(2048), { "content-length": "10" });
    expect([400, 413]).toContain(res.status);
  });
});
