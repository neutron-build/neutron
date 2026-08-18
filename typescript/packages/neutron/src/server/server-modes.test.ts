import * as fs from "node:fs/promises";
import * as net from "node:net";
import * as path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { WebSocket } from "ws";
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

// A throwaway root with no src/routes — proves non-ssr modes never touch the routes dir.
async function emptyRoot(): Promise<string> {
  return await fs.mkdtemp(path.join(process.cwd(), ".tmp-neutron-modes-"));
}

describe("server modes", () => {
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

  it("raw mode: no SSR machinery, /health works, user routes work, default 404", async () => {
    root = await emptyRoot();
    const port = await getFreePort();
    // No throw despite missing src/routes — discovery is skipped in non-ssr modes.
    running = await createServer({
      mode: "raw",
      host: "127.0.0.1",
      port,
      rootDir: root,
      compress: false,
    });
    expect(running.wss).toBeUndefined();

    // Caller mounts a route on the returned app after createServer returns.
    running.app.get("/ping", (c) => c.json({ pong: true }));
    const base = `http://127.0.0.1:${port}`;

    const health = await fetch(`${base}/health`);
    expect(health.status).toBe(200);
    expect((await health.json()).status).toBe("ok");

    const ping = await fetch(`${base}/ping`);
    expect(ping.status).toBe(200);
    expect(await ping.json()).toEqual({ pong: true });

    // No asset serving in raw mode.
    expect((await fetch(`${base}/assets/app.js`)).status).toBe(404);
    // Hono's default 404 for anything unmatched.
    expect((await fetch(`${base}/nope`)).status).toBe(404);
  });

  it("api mode: RFC 7807 404, user routes still reachable (notFound, not a shadowing catch-all)", async () => {
    root = await emptyRoot();
    const port = await getFreePort();
    running = await createServer({
      mode: "api",
      host: "127.0.0.1",
      port,
      rootDir: root,
      compress: false,
    });
    // Mounted AFTER createServer — must not be shadowed by the 404 handler.
    running.app.get("/api/items", (c) => c.json([1, 2, 3]));
    const base = `http://127.0.0.1:${port}`;

    const items = await fetch(`${base}/api/items`);
    expect(items.status).toBe(200);
    expect(await items.json()).toEqual([1, 2, 3]);

    const missing = await fetch(`${base}/missing`);
    expect(missing.status).toBe(404);
    // FRAMEWORK_CONTRACT.md §2: errors are problem+json, not ad-hoc JSON.
    expect(missing.headers.get("content-type")).toContain("application/problem+json");
    expect(await missing.json()).toEqual({
      type: "https://neutron.dev/errors/not-found",
      title: "Not Found",
      status: 404,
      detail: "No route matches /missing",
      instance: "/missing",
    });

    expect((await fetch(`${base}/health`)).status).toBe(200);
  });

  it("default mode is ssr and exposes no wss", async () => {
    root = await emptyRoot();
    const port = await getFreePort();
    running = await createServer({
      host: "127.0.0.1",
      port,
      rootDir: root,
      compress: false,
    });
    expect(running.wss).toBeUndefined();
    expect((await fetch(`http://127.0.0.1:${port}/health`)).status).toBe(200);
  });

  it("websocket: true attaches a wss that accepts any path and echoes", async () => {
    root = await emptyRoot();
    const port = await getFreePort();
    running = await createServer({
      mode: "raw",
      host: "127.0.0.1",
      port,
      rootDir: root,
      compress: false,
      websocket: true,
    });
    expect(running.wss).toBeDefined();
    running.wss!.on("connection", (ws) => {
      ws.on("message", (data) => ws.send(data.toString()));
    });

    const echoed = await new Promise<string>((resolve, reject) => {
      const client = new WebSocket(`ws://127.0.0.1:${port}/anything`);
      client.on("open", () => client.send("hi"));
      client.on("message", (data) => {
        resolve(data.toString());
        client.close();
      });
      client.on("error", reject);
    });
    expect(echoed).toBe("hi");
  });

  it("websocket: { path } only upgrades the configured path", async () => {
    root = await emptyRoot();
    const port = await getFreePort();
    running = await createServer({
      mode: "raw",
      host: "127.0.0.1",
      port,
      rootDir: root,
      compress: false,
      websocket: { path: "/ws" },
    });
    running.wss!.on("connection", (ws) => ws.send("welcome"));

    const onPath = await new Promise<string>((resolve, reject) => {
      const client = new WebSocket(`ws://127.0.0.1:${port}/ws`);
      client.on("message", (data) => {
        resolve(data.toString());
        client.close();
      });
      client.on("error", reject);
    });
    expect(onPath).toBe("welcome");

    // A different path is rejected at the upgrade — the client errors instead of opening.
    const rejected = await new Promise<boolean>((resolve) => {
      const client = new WebSocket(`ws://127.0.0.1:${port}/other`);
      client.on("open", () => {
        client.close();
        resolve(false);
      });
      client.on("error", () => resolve(true));
    });
    expect(rejected).toBe(true);
  });

  it("close() drains promptly even with a live WebSocket connection", async () => {
    root = await emptyRoot();
    const port = await getFreePort();
    const server = await createServer({
      mode: "raw",
      host: "127.0.0.1",
      port,
      rootDir: root,
      compress: false,
      websocket: true,
    });
    server.wss!.on("connection", () => {
      // Hold the socket open and idle — never close it from the server side.
    });

    // Establish a live connection and leave it open.
    await new Promise<void>((resolve, reject) => {
      const client = new WebSocket(`ws://127.0.0.1:${port}/`);
      client.on("open", () => resolve());
      client.on("error", reject);
    });

    // close() must not hang on the live socket; it should resolve well under any
    // shutdown timeout because we terminate WS clients before draining HTTP.
    const start = Date.now();
    await Promise.race([
      server.close(),
      new Promise<never>((_, reject) =>
        setTimeout(() => reject(new Error("close() hung on a live WS connection")), 3000)
      ),
    ]);
    expect(Date.now() - start).toBeLessThan(3000);
    // Already closed above — keep afterEach from double-closing.
    running = null;
  });
});
