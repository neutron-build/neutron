import * as fs from "node:fs/promises";
import * as net from "node:net";
import * as path from "node:path";
import { Hono } from "hono";
import { afterEach, describe, expect, it } from "vitest";
import { createServer, type NeutronServer, type NeutronServerOptions } from "./index.js";

/**
 * Global middleware (src/middleware.ts) runs before mounted sub-apps.
 * FRAMEWORK_CONTRACT §5 puts authentication before the route handler with no
 * exemption, and a mount is a route handler: an auth gate in src/middleware.ts
 * used to silently not cover `mount` APIs at all.
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

// Counts its runs per path so a fall-through can be shown to run it once.
const GATE = `
export default async function middleware(request, context, next) {
  const url = new URL(request.url);
  const runs = (globalThis.__neutronMountGateRuns ??= {});
  runs[url.pathname] = (runs[url.pathname] ?? 0) + 1;
  if (request.headers.get("x-token") !== "ok") {
    return new Response("login required", { status: 403, headers: { "x-gate": "denied" } });
  }
  if (request.headers.get("x-replace") === "1") {
    await next();
    return new Response("replaced by middleware", { status: 202 });
  }
  const response = await next();
  response.headers.set("x-gate", "passed " + request.method + " " + url.pathname + url.search);
  return response;
}
`;

const PAGE = `
import { h } from "preact";
export const config = { mode: "app" };
export default function Page() { return h("p", null, "home"); }
`;

function runs(): Record<string, number> {
  return ((globalThis as Record<string, unknown>).__neutronMountGateRuns ?? {}) as Record<
    string,
    number
  >;
}

describe("global middleware on mounted sub-apps", () => {
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
    delete (globalThis as Record<string, unknown>).__neutronMountGateRuns;
  });

  async function boot(mount: NeutronServerOptions["mount"]): Promise<string> {
    root = await fs.mkdtemp(path.join(process.cwd(), ".tmp-neutron-mount-gate-"));
    await fs.mkdir(path.join(root, "src", "routes"), { recursive: true });
    await fs.mkdir(path.join(root, "dist"), { recursive: true });
    await fs.writeFile(path.join(root, "src", "middleware.ts"), GATE, "utf-8");
    await fs.writeFile(path.join(root, "src", "routes", "index.tsx"), PAGE, "utf-8");
    const port = await getFreePort();
    running = await createServer({ host: "127.0.0.1", port, rootDir: root, compress: false, mount });
    return `http://127.0.0.1:${port}`;
  }

  function api(): Hono {
    const sub = new Hono();
    sub.get("/y", (c) => c.text("ok", 200, { "x-sub-app": "1" }));
    sub.post("/echo", async (c) => c.text("echo:" + (await c.req.text())));
    return sub;
  }

  it("gates a mount by default and passes the real request through", { timeout: 30_000 }, async () => {
    const base = await boot({ "/x": api() });

    const denied = await fetch(`${base}/x/y`);
    expect(denied.status).toBe(403);
    expect(denied.headers.get("x-gate")).toBe("denied");
    expect(await denied.text()).toBe("login required");

    const allowed = await fetch(`${base}/x/y?q=1`, { headers: { "x-token": "ok" } });
    expect(allowed.status).toBe(200);
    expect(await allowed.text()).toBe("ok");
    expect(allowed.headers.get("x-gate")).toBe("passed GET /x/y?q=1");
    expect(allowed.headers.get("x-sub-app")).toBe("1");

    // The sub-app still reads the body the middleware let through.
    const posted = await fetch(`${base}/x/echo`, {
      method: "POST",
      headers: { "x-token": "ok", origin: base },
      body: "payload",
    });
    expect(await posted.text()).toBe("echo:payload");
    expect(runs()["/x/y"]).toBe(2);
  });

  it("lets a middleware's own response replace the sub-app's", { timeout: 30_000 }, async () => {
    const base = await boot({ "/x": api() });

    const replaced = await fetch(`${base}/x/y`, { headers: { "x-token": "ok", "x-replace": "1" } });
    expect(replaced.status).toBe(202);
    expect(await replaced.text()).toBe("replaced by middleware");
    // Nothing of the discarded sub-app response leaks into the replacement.
    expect(replaced.headers.get("x-sub-app")).toBeNull();
  });

  it("runs the chain once when a mount path falls through to the routes", { timeout: 30_000 }, async () => {
    const base = await boot({ "/x": api() });

    expect((await fetch(`${base}/x/missing`)).status).toBe(403);
    const missing = await fetch(`${base}/x/missing`, {
      headers: { "x-token": "ok", accept: "application/json" },
    });
    expect(missing.status).toBe(404);
    expect(missing.headers.get("x-gate")).toBe("passed GET /x/missing");
    expect(runs()["/x/missing"]).toBe(2);
  });

  it("skips the chain for a mount that opts out", { timeout: 30_000 }, async () => {
    const base = await boot({
      "/x": api(),
      "/self-gated": { app: api(), globalMiddleware: false },
    });

    const open = await fetch(`${base}/self-gated/y`);
    expect(open.status).toBe(200);
    expect(await open.text()).toBe("ok");
    expect(open.headers.get("x-gate")).toBeNull();
    expect(runs()["/self-gated/y"]).toBeUndefined();

    // The opt-out is per mount, and pages keep their gate.
    expect((await fetch(`${base}/x/y`)).status).toBe(403);
    expect((await fetch(`${base}/`)).status).toBe(403);
  });

  it("rejects an object mount entry without a Hono app", async () => {
    root = await fs.mkdtemp(path.join(process.cwd(), ".tmp-neutron-mount-gate-"));
    await expect(
      createServer({
        rootDir: root,
        mode: "api",
        port: await getFreePort(),
        host: "127.0.0.1",
        mount: { "/x": { app: (() => new Response("no")) as never } },
      })
    ).rejects.toThrow(/is not a Hono instance/);
  });
});
