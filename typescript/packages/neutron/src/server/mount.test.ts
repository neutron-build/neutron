import * as fs from "node:fs/promises";
import * as net from "node:net";
import * as path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { WebSocket } from "ws";
import { Hono } from "hono";
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

async function makeRoot(): Promise<string> {
  return await fs.mkdtemp(path.join(process.cwd(), ".tmp-neutron-mount-"));
}

/** An ssr-shaped fixture: a prebuilt static page, an app-mode page, assets. */
async function writeSsrFixture(rootDir: string): Promise<void> {
  await fs.mkdir(path.join(rootDir, "dist", "assets"), { recursive: true });
  await fs.mkdir(path.join(rootDir, "src", "routes", "api"), { recursive: true });

  await fs.writeFile(
    path.join(rootDir, "dist", "index.html"),
    "<!doctype html><html><body><h1>Mount Fixture Home</h1></body></html>",
    "utf-8"
  );
  await fs.writeFile(
    path.join(rootDir, "dist", "assets", "app.js"),
    "console.log('mount fixture asset');",
    "utf-8"
  );

  // A page under the mount prefix, served by the route table. The mount must
  // fall through to it for paths the sub-app does not answer.
  await fs.writeFile(
    path.join(rootDir, "src", "routes", "api", "page.ts"),
    `
import { h } from "preact";
export const config = { mode: "app" };
export async function loader() {
  return { via: "route-table" };
}
export default function ApiPage({ data }) {
  return h("div", null, "api page from route table: " + data.via);
}
`,
    "utf-8"
  );
}

function jsonSubApp(): Hono {
  const subApp = new Hono();
  subApp.get("/hello", (c) => c.json({ hello: "mount" }));
  subApp.get("/echo/:id", (c) => c.json({ id: c.req.param("id") }));
  return subApp;
}

describe("createServer mount option (Option B — in-process API routes)", () => {
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

  it("ssr mode: mounted sub-app and SSR pages coexist in one server", async () => {
    root = await makeRoot();
    await writeSsrFixture(root);
    const port = await getFreePort();
    running = await createServer({
      host: "127.0.0.1",
      port,
      rootDir: root,
      compress: false,
      mount: { "/api": jsonSubApp() },
    });
    const base = `http://127.0.0.1:${port}`;

    // The mount answers under its prefix (not shadowed by the HTML catch-all).
    const hello = await fetch(`${base}/api/hello`);
    expect(hello.status).toBe(200);
    expect(await hello.json()).toEqual({ hello: "mount" });

    // Path params inside the sub-app keep working.
    const echo = await fetch(`${base}/api/echo/42`);
    expect(await echo.json()).toEqual({ id: "42" });

    // The static fast path is unaffected: prebuilt HTML still serves at /.
    const home = await fetch(`${base}/`);
    expect(home.status).toBe(200);
    expect(await home.text()).toContain("Mount Fixture Home");

    // Framework battery still answers (mount must not shadow it).
    const health = await fetch(`${base}/health`);
    expect(health.status).toBe(200);
    expect((await health.json()).status).toBe("ok");
  });

  it("ssr mode: mount falls through to the route table where the sub-app has no route", async () => {
    root = await makeRoot();
    await writeSsrFixture(root);
    const port = await getFreePort();
    running = await createServer({
      host: "127.0.0.1",
      port,
      rootDir: root,
      compress: false,
      mount: { "/api": jsonSubApp() },
    });
    const base = `http://127.0.0.1:${port}`;

    // /api/page is NOT in the sub-app — the request falls through the mount to
    // the HTML catch-all, which serves the route-table page (app mode, loader).
    const page = await fetch(`${base}/api/page`);
    expect(page.status).toBe(200);
    expect(await page.text()).toContain("api page from route table: route-table");
  });

  it("ssr mode: a route-table page under the prefix loses to a mount route at the same path", async () => {
    root = await makeRoot();
    await writeSsrFixture(root);
    const subApp = new Hono();
    subApp.get("/page", (c) => c.json({ winner: "mount" }));
    const port = await getFreePort();
    running = await createServer({
      host: "127.0.0.1",
      port,
      rootDir: root,
      compress: false,
      mount: { "/api": subApp },
    });
    const base = `http://127.0.0.1:${port}`;

    // Both the route table and the mount can reach /api/page. Mounts register
    // pre-catch-all, so the mount wins — deterministic, documented order.
    const res = await fetch(`${base}/api/page`);
    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ winner: "mount" });
  });

  it("ssr mode: static asset fast path unaffected (dist assets still serve)", async () => {
    root = await makeRoot();
    await writeSsrFixture(root);
    const port = await getFreePort();
    running = await createServer({
      host: "127.0.0.1",
      port,
      rootDir: root,
      compress: false,
      mount: { "/api": jsonSubApp() },
    });
    const base = `http://127.0.0.1:${port}`;

    const asset = await fetch(`${base}/assets/app.js`);
    expect(asset.status).toBe(200);
    expect(await asset.text()).toContain("mount fixture asset");
  });

  it("ssr mode: mount does not shadow /health even when adjacent prefixes look similar", async () => {
    root = await makeRoot();
    await writeSsrFixture(root);
    const subApp = new Hono();
    subApp.get("/status", (c) => c.json({ sub: "app" }));
    const port = await getFreePort();
    running = await createServer({
      host: "127.0.0.1",
      port,
      rootDir: root,
      compress: false,
      mount: { "/healthz": subApp },
    });
    const base = `http://127.0.0.1:${port}`;

    // /health is the framework's; /healthz/status is the mount's.
    const health = await fetch(`${base}/health`);
    expect(health.status).toBe(200);
    const body = (await health.json()) as { status: string; nucleus: string };
    expect(body.status).toBe("ok");
    expect(body.nucleus).toBe("unconfigured");

    const sub = await fetch(`${base}/healthz/status`);
    expect(sub.status).toBe(200);
    expect(await sub.json()).toEqual({ sub: "app" });
  });

  it("api mode: mount answers and unmatched paths stay RFC 7807 problem+json", async () => {
    root = await makeRoot();
    const port = await getFreePort();
    running = await createServer({
      mode: "api",
      host: "127.0.0.1",
      port,
      rootDir: root,
      compress: false,
      mount: { "/api": jsonSubApp() },
    });
    const base = `http://127.0.0.1:${port}`;

    const hello = await fetch(`${base}/api/hello`);
    expect(hello.status).toBe(200);
    expect(await hello.json()).toEqual({ hello: "mount" });

    // Falls through the mount to the api-mode notFound handler.
    const missing = await fetch(`${base}/api/missing`);
    expect(missing.status).toBe(404);
    expect(missing.headers.get("content-type")).toContain("application/problem+json");
    const problem = (await missing.json()) as Record<string, unknown>;
    expect(problem.type).toBe("https://neutron.dev/errors/not-found");
    expect(problem.status).toBe(404);
    expect(problem.instance).toBe("/api/missing");

    expect((await fetch(`${base}/health`)).status).toBe(200);
  });

  it("raw mode: mount works alongside caller routes", async () => {
    root = await makeRoot();
    const port = await getFreePort();
    running = await createServer({
      mode: "raw",
      host: "127.0.0.1",
      port,
      rootDir: root,
      compress: false,
      mount: { "/api": jsonSubApp() },
    });
    running.app.get("/ping", (c) => c.json({ pong: true }));
    const base = `http://127.0.0.1:${port}`;

    expect(await (await fetch(`${base}/api/hello`)).json()).toEqual({ hello: "mount" });
    expect(await (await fetch(`${base}/ping`)).json()).toEqual({ pong: true });
  });

  it("mount is orthogonal to the websocket axis", async () => {
    root = await makeRoot();
    const port = await getFreePort();
    running = await createServer({
      mode: "raw",
      host: "127.0.0.1",
      port,
      rootDir: root,
      compress: false,
      websocket: true,
      mount: { "/api": jsonSubApp() },
    });
    running.wss!.on("connection", (ws) => ws.send("welcome"));
    const base = `http://127.0.0.1:${port}`;

    expect(await (await fetch(`${base}/api/hello`)).json()).toEqual({ hello: "mount" });

    const greeted = await new Promise<string>((resolve, reject) => {
      const client = new WebSocket(`ws://127.0.0.1:${port}/ws`);
      client.on("message", (data) => {
        resolve(data.toString());
        client.close();
      });
      client.on("error", reject);
    });
    expect(greeted).toBe("welcome");
  });

  it("rejects nested mount prefixes", async () => {
    root = await makeRoot();
    const port = await getFreePort();
    await expect(
      createServer({
        host: "127.0.0.1",
        port,
        rootDir: root,
        compress: false,
        mount: { "/api": jsonSubApp(), "/api/v1": new Hono() },
      })
    ).rejects.toThrow(/"\/api" and "\/api\/v1" overlap/);
  });

  it("rejects mounting over /health", async () => {
    root = await makeRoot();
    const port = await getFreePort();
    await expect(
      createServer({
        host: "127.0.0.1",
        port,
        rootDir: root,
        compress: false,
        mount: { "/health": jsonSubApp() },
      })
    ).rejects.toThrow(/"\/health" would take over GET \/health/);
  });

  it("rejects mounting at the site root with a pointer to raw mode", async () => {
    root = await makeRoot();
    const port = await getFreePort();
    await expect(
      createServer({
        host: "127.0.0.1",
        port,
        rootDir: root,
        compress: false,
        mount: { "/": jsonSubApp() },
      })
    ).rejects.toThrow(/mode: "raw"/);
  });

  it("rejects mount prefixes that cover ssr static trees", async () => {
    root = await makeRoot();
    const port = await getFreePort();
    await expect(
      createServer({
        host: "127.0.0.1",
        port,
        rootDir: root,
        compress: false,
        mount: { "/assets": new Hono() },
      })
    ).rejects.toThrow(/"\/assets" would take over/);
  });

  it("allows ssr-reserved prefixes in non-ssr modes", async () => {
    root = await makeRoot();
    const port = await getFreePort();
    const subApp = new Hono();
    subApp.get("/x", (c) => c.json({ ok: true }));
    // /assets has no meaning in api mode — mounting there is fine.
    running = await createServer({
      mode: "api",
      host: "127.0.0.1",
      port,
      rootDir: root,
      compress: false,
      mount: { "/assets": subApp },
    });
    const res = await fetch(`http://127.0.0.1:${port}/assets/x`);
    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ ok: true });
  });

  it("rejects a trailing slash in the prefix", async () => {
    root = await makeRoot();
    const port = await getFreePort();
    await expect(
      createServer({
        host: "127.0.0.1",
        port,
        rootDir: root,
        compress: false,
        mount: { "/api/": jsonSubApp() },
      })
    ).rejects.toThrow(/must not end with "\/" — write "\/api"/);
  });

  it("rejects a non-Hono mount value", async () => {
    root = await makeRoot();
    const port = await getFreePort();
    await expect(
      createServer({
        host: "127.0.0.1",
        port,
        rootDir: root,
        compress: false,
        mount: { "/api": (() => undefined) as unknown as Hono },
      })
    ).rejects.toThrow(/not a Hono instance/);
  });

  it("rejects dynamic segments in the prefix", async () => {
    root = await makeRoot();
    const port = await getFreePort();
    await expect(
      createServer({
        host: "127.0.0.1",
        port,
        rootDir: root,
        compress: false,
        mount: { "/api/:tenant": jsonSubApp() },
      })
    ).rejects.toThrow(/must be literal/);
  });
});
